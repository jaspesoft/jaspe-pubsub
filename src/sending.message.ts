import { PubSub } from "@google-cloud/pubsub";

export class SendingMessage<T> {
  private pubSubClient: PubSub;

  constructor(private readonly topic: string) {
    if (process.env.KEY_FILE_CREDENTIALS) {
      this.pubSubClient = new PubSub({
        projectId: process.env.PROJECT_ID,
        keyFile: process.env.KEY_FILE_CREDENTIALS,
      });
    } else {
      this.pubSubClient = new PubSub();
    }
  }

  static instance(topic: string) {
    return new SendingMessage(topic);
  }

  async publish(data: any) {
    const dataBuffer = Buffer.from(JSON.stringify(data));

    try {
      const messageId = await this.pubSubClient
        .topic(this.topic)
        .publishMessage({ data: dataBuffer });

      console.log(`Message ${messageId} published.`);
    } catch (error) {
      console.error(`Received error while publishing: ${error.message}`);
    }
  }
}
