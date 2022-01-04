import { PubSub, Topic } from "@google-cloud/pubsub";

export class WorkerSubcription {
  private pubSubClient: PubSub;

  constructor(private readonly subscription: string) {
    if (process.env.KEY_FILE_CREDENTIALS) {
      this.pubSubClient = new PubSub({
        projectId: process.env.PROJECT_ID,
        keyFile: process.env.KEY_FILE_CREDENTIALS,
      });
    } else {
      this.pubSubClient = new PubSub();
    }
  }

  static instance(subscription: string): WorkerSubcription {
    return new WorkerSubcription(subscription);
  }

  public subscribe(callback: Function, timeout = 60) {
    timeout = Number(timeout);
    const subscription = this.pubSubClient.subscription(this.subscription);

    let messageCount = 0;
    const messageHandler = (message: any) => {
      messageCount++;
      console.log(`Received message id: ${message.id}\t`);
      console.log(`Received message Data: ${message.data}`);
      console.log(`\tAttributes: ${message.attributes}`);
      message.ack();
      if (messageCount === 1) {
        callback(message.data);
      }
    };

    subscription.on("message", messageHandler);
    setTimeout(() => {
      messageCount = 0;
      console.log(`${messageCount} message(s) received.`);
    }, timeout * 1000);
  }
}
