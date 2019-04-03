import Mailchimp from "mailchimp-api-v3";
import * as dynamoDbLib from "./libs/dynamodb-lib";
import { getUserNewsletter, findNextLE, updateCampaignContent } from "./libs/utils";

export async function main() { 

    async function getUserTable() {

        const scan_params = {
            TableName: "tr-users",
            ProjectionExpression: "firstName, lastName, userId, emailAddress, delayDays, delayTime, apiKey, listId, templateId"
        };

        try {
            return await dynamoDbLib.call("scan", scan_params)
                .then((result) => {
                    return result.Items
                })
        } catch (err) {
            console.log("An error occured fetching the user table.", err);
        }
    }

    async function scheduleCampaign(user, launchEvent, mailchimp){
        try {
            var tMinusZero = new Date(launchEvent.launchDate);
            if(user.delayDays) tMinusZero.setDate(tMinusZero.getDate() + user.delayDays);
            var zuluTime = new Date(tMinusZero);
            var pacificTime = new Date(zuluTime.toLocaleString("en-US",{timeZone: "America/Los_Angeles"}));
            var offset = Math.round((zuluTime-pacificTime)/1000/60/60,0); //get the pacific/zulu offset in hours...
            if(user.delayTime & user.delayTime !== "null") {
                tMinusZero.setHours(+user.delayTime.substring(0,2) + offset, user.delayTime.substring(2));
            } else {
                tMinusZero.setHours(10 + offset,0,0,0);
            }
            const mc_request = {
                method: "post",
                path: `/campaigns/${user.campaignId}/actions/schedule`,
                body: {
                    schedule_time: tMinusZero.toISOString()                       
                }
            }
            console.log("mc_request for: "+user.emailAddress+"...",mc_request);
            return await mailchimp.request(mc_request)
                .then((result) => {
                    console.log("Campaign successfully scheduled for "+user.emailAddress+"...",result);
                    return result;
                })
        } catch (err) {
            console.log("An error occured scheduling the campaign on MC servers for "+user.emailAddress+"...",err);
        }
    }

    async function createMcCampaign (user) {
        const mailchimp = new Mailchimp(user.apiKey);
        try {
            //this little ditty is going to create the campaign, we'll need to update the content once a campaign has been created...
            return await mailchimp.request({ 
                method: "post",
                path: "/campaigns",
                body: {
                    type: "regular",
                    recipients: {
                        list_id: user.listId
                    },
                    settings: {
                        subject_line: user.subject,
                        title: "Trusty Raven Newsletter",
                        from_name: `${user.firstName} ${user.lastName}`,
                        reply_to: user.emailAddress,
                        template_id: user.templateId
                    }                            
                }
            })
                .then((result) => {
                    user.campaignId = result.id;
                    console.log("Campaign created for "+user.emailAddress+"...", result.id);
                    return {user: user, mailchimp: mailchimp};
                })

        } catch (err) {
            console.log("An error occured creating the campaign with mc for "+user.emailAddress+"...",err);
        }
    }

    async function scheduleUserNewsletter (user, launchEvent) {

        //first, get the user newsletter...
        try {    
            await getUserNewsletter(user, launchEvent.defaultNewsletter)
                .then((result) => {
                    user = result;
                    console.log("User Newsletter Retrieved for "+user.emailAddress+"...");
                })
        } catch (err) {
            console.log("An error occured fetching user newsletter for "+user.emailAddress+"...",err);
        }

        //next, create the campaign on mc
        var mailchimp;
        await createMcCampaign(user)
            .then((result) => {
                user = result.user;
                mailchimp = result.mailchimp;
            })

        //next up, push the campaign ID to the user record on dynamodb...

        const put_params = {
            TableName: "tr-users",
            Key: {
                userId: user.userId
            },
            UpdateExpression: "SET campaignId = :campaignId",
            ExpressionAttributeValues: {
                ":campaignId": user.campaignId
            },
            ReturnValues: "ALL_NEW"
        };
        try {
            await dynamoDbLib.call("update", put_params)
                .then((result) => {
                    console.log("Campaign ID added to user record for "+user.emailAddress+"...", result);
                })
        } catch (err) {
            console.log("An error occured updating the user record with the campaignId for "+user.emailAddress+"...",err);
        }

        //next up, we'll udpate the campaign content with the correct verbiage...
        try {
            await updateCampaignContent(user, user.newsletter, user.subject, mailchimp)
                .then((result) => {
                    console.log("Campaign Content added for "+user.emailAddress+"...", result);
                })
        } catch (err) {
            console.log("An error occured updating campaign content on MC servers for "+user.emailAddress+"...",err);
        }

        //finally, let's schedule the campaign...
        //mailchimp needs half a second to digest the campaign before you can schedule it. Lame.
        return setTimeout(() => {scheduleCampaign(user, launchEvent, mailchimp)}, 500);
    }
    

    try {
        //first get all the active users...
        await getUserTable()
            .then( async (userTable) => {
                //next get the next launch event
                var launchEvent;
                await findNextLE()
                    .then((result) => {
                        console.log("Launch Event: ",result);
                        launchEvent = result;
                    })

                //Schedule for 5 users at a time because MC limits simultaneous connections. lame...
                for (let i = 0; i < userTable.length; i+=5) {
                    var batch = [];
                    for (let ii = i; (ii < userTable.length && ii < i+5); ii++) {
                        batch.push(userTable[ii]);
                    }
                    await Promise.all(batch.map( async user => { await scheduleUserNewsletter(user, launchEvent) }))
                }
            })
    } catch (e) {
        console.dir(e);
    }
}