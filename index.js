const { eventToSignal } = require("./signal");
const { signalToMilestone } = require("./milestone");
const dotenv = require("dotenv");
dotenv.config();

exports.handler = async (event) => {
  const env = process.env.NODE_ENV;
  const trigger = (event && event.trigger) || "unknown";
  const isCronTrigger = trigger === "cron";

  if (isCronTrigger) {
    // Handle cron invocation separately.
    console.log("Invocation trigger: cronjob");
    return { statusCode: 200, body: JSON.stringify({ message: "cron trigger acknowledged" }) };
  }

  // Non-cron invocation; continue with normal handling.
  let events = [];

  if (env === "production") {
    if (!Array.isArray(event.Records)) {
      console.error("Expected Records array in event payload");
      return;
    }

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

      // ======== 🔥 START SECTION — MINIMAL + SAFE ========

      // Extract userId from payload.data
      let userId = payload?.data?.userId;

      // Fallback: extract from Firestore document path
      // Example: "app_usage_events/<userId>/events/<docId>"
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

      // Build the event object using your EXPECTED structure
      events.push({
        ...payload.data, // preserve all Firestore fields for your logic
        userId, // ensure userId is ALWAYS present
        timestamp, // ensure timestamp is ALWAYS present
        collection: payload.collection, // keep collection consistent with your old code
      });

      // ======== 🔥 END SECTION ========
    }
  } else {
    // ======== DEVELOPMENT MOCK EVENT ========
    events = [
      {
        androidVersion: "15",
        date: "2025-09-12",
        deviceModel: "samsung SM-S928B",
        eventType: "AGGREGATE",
        isScheduled: true,
        lastTimeUsed: 0,
        lastTimeUsedFormatted: "1970-01-01 00:00:00",
        packageName: "inc.loop.ppt",
        phoneNumber: "",
        rank: 0,
        sessionId: "a2556448-8992-48c1-bb6f-49f133f62f8c",
        totalTimeInForeground: 0,
        totalTimeInForegroundMinutes: 0,
        totalTimeInForegroundMs: 300000,
        userId: "03dc138a-04da-4a0c-bd19-e9efbf238407",
        username: "",
      },
    ];
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
