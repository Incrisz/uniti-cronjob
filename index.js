const { eventToSignal } = require("./signal");
const { signalToMilestone } = require("./milestone");
const dotenv = require("dotenv");
dotenv.config();

exports.handler = async (event) => {
  const env = process.env.NODE_ENV;
  const trigger = (event && event.trigger) || "unknown";
  const isCronTrigger = trigger === "cron" || trigger === "cronjob";

  console.log(`Invocation trigger: ${trigger}`);

  // If cron-triggered or no data to process, short-circuit.
  if (isCronTrigger || !Array.isArray(event.Records) || event.Records.length === 0) {
    console.log("No Kinesis records to process; exiting.");
    return { statusCode: 200, body: JSON.stringify({ message: "cronjob is active", trigger }) };
  }

  // Non-cron invocation with Kinesis records.
  let events = [];

  for (const record of event.Records) {
    const decodedStr = Buffer.from(record.kinesis.data, "base64").toString(
      "utf8"
    );
    const payload = JSON.parse(decodedStr);

    // Log raw Kinesis payload
    console.log(JSON.stringify(payload));

    // Only handle added/modified mutations; ignore anything else defensively.
    const mutation = (payload.event || "").toLowerCase();
    if (mutation && !["added", "modified"].includes(mutation)) {
      console.log(`Skipping mutation type: ${payload.event}`);
      continue;
    }

    // Extract userId from payload.data
    let userId = payload?.data?.userId;
    if (!userId && payload.document_path) {
      const parts = payload.document_path.split("/");
      if (parts.length >= 2) {
        userId = parts[1];
      }
    }

    // Extract timestamp from correct Firestore fields
    const timestamp =
      payload?.data?.timestamp ||
      payload?.data?.timestampFormatted ||
      payload.timestamp ||
      Date.now();

    events.push({
      ...payload.data,
      userId,
      timestamp,
      collection: payload.collection,
    });
  }

  // ======== PROCESS EVENTS  ========
  for (const event of events) {
    try {
      const signals = await eventToSignal(event);
      await signalToMilestone(event, signals);
    } catch (err) {
      console.error("Error processing event:", err);
      return {
        statusCode: 500,
        body: JSON.stringify({ error: err.message }),
      };
    }
  }
};
