import { PubSubEngine } from 'graphql-subscriptions';
import { connect, Client, ClientOpts, SubscribeOptions, NatsError } from 'nats';
import { PubSubAsyncIterator } from './pubsub-async-iterator';
import _ from 'lodash';

export type Path = Array<string | number>;
export type Trigger = string | Path;
export type TriggerTransform = (trigger: Trigger, channelOptions?: object) => string;
export type SubscribeOptionsResolver = (trigger: Trigger, channelOptions?: object) => Promise<SubscribeOptions>;
export type PublishOptionsResolver = (trigger: Trigger, payload: any) => Promise<any>;

export interface NatsPubSubOptions {
    client?: Client;
    subscribeOptions?: SubscribeOptionsResolver;
    publishOptions?: PublishOptionsResolver;
    connectionListener?: (err: Error) => void;
    // onNatsSubscribe?: (id: number, granted: ISubscriptionGrant[]) => void;
    triggerTransform?: TriggerTransform;
    parseMessageWithEncoding?: BufferEncoding;
}

export class NatsPubSub implements PubSubEngine {

    private triggerTransform: TriggerTransform;
    private publishOptionsResolver: PublishOptionsResolver;
    private subscribeOptionsResolver: SubscribeOptionsResolver;
    private natsConnection: Client;

    // { [subId]: {topic, natsSid, onMessage} } -- NATS Subscriptions
    private subscriptionMap: { [subId: number]: [string, Function] };
    // { [topic]: [ subId1, subId2, ...]}
    private subsRefsMap: { [trigger: string]: Array<number> };
    // { [topic]: { natsSid }}
    private natsSubMap: { [trigger: string]: number };
    private currentSubscriptionId: number;

    public constructor(options: NatsPubSubOptions = {}) {
        this.triggerTransform = options.triggerTransform || (trigger => trigger as string);

        if (options.client) {
            this.natsConnection = options.client;
        } else {
            const brokerUrl = 'nats://127.0.0.1:4222';
            this.natsConnection = connect(brokerUrl);
        }


        if (options.connectionListener) {
            this.natsConnection.on('connect', options.connectionListener);
            this.natsConnection.on('error', options.connectionListener);
            this.natsConnection.on('disconnect', options.connectionListener);
            this.natsConnection.on('reconnecting', options.connectionListener);
            this.natsConnection.on('reconnect', options.connectionListener);
            this.natsConnection.on('close', options.connectionListener);
        }

        this.subscriptionMap = {};
        this.subsRefsMap = {};
        this.natsSubMap = {};
        this.currentSubscriptionId = 0;
        // this.onNatsSubscribe = options.onNatsSubscribe || (() => null);
        this.publishOptionsResolver = options.publishOptions || (() => Promise.resolve({}));
        this.subscribeOptionsResolver = options.subscribeOptions || (() => Promise.resolve({}));
    }

    public async publish(trigger: string, payload: any): Promise<void> {
        const message = JSON.stringify(payload);
        await this.natsConnection.publish(trigger, message);
    }

    public async subscribe(trigger: string, onMessage: Function, options?: object): Promise<number> {
        const triggerName: string = this.triggerTransform(trigger, options);
        const id = this.currentSubscriptionId++;
        this.subscriptionMap[id] = [triggerName, onMessage];

        let refs = this.subsRefsMap[triggerName];
        if (refs && refs.length > 0) {
            const newRefs = [...refs, id];
            this.subsRefsMap[triggerName] = newRefs;
            return await id;
        } else {
            // return new Promise<number>((resolve, reject) => {
            // 1. Resolve options object
            // this.subscribeOptionsResolver(trigger, options).then(subscriptionOptions => {
            // 2. Subscribing using NATS
            const subId = this.natsConnection.subscribe(triggerName, (msg) => this.onMessage(triggerName, msg));
            this.subsRefsMap[triggerName] = [...(this.subsRefsMap[triggerName] || []), id];
            this.natsSubMap[triggerName] = subId;
            return await id;
            // });
            // });

        }
    }

    public unsubscribe(subId: number) {
        const [triggerName = null] = this.subscriptionMap[subId] || [];
        const refs = this.subsRefsMap[triggerName];
        const natsSubId = this.natsSubMap[triggerName];
        if (!refs) {
            throw new Error(`There is no subscription of id "${subId}"`);
        }
        if (refs.length === 1) {
            this.natsConnection.unsubscribe(natsSubId);
            delete this.natsSubMap[triggerName];
            delete this.subsRefsMap[triggerName];
        } else {
            const index = refs.indexOf(subId);
            const newRefs = index === -1 ? refs : [...refs.slice(0, index), ...refs.slice(index + 1)];
            this.subsRefsMap[triggerName] = newRefs;
        }

        delete this.subscriptionMap[subId];
    }

    public asyncIterator<T>(triggers: string | string[]): AsyncIterator<T> {
        return new PubSubAsyncIterator<T>(this, triggers);
    }

    private onMessage(topic: string, message: string) {
        const subscribers = this.subsRefsMap[topic];

        // Don't work for nothing..
        if (!subscribers || !subscribers.length) {
            return;
        }

        // for example a client could inject a bogus `toString` property
        // which could cause your client to crash should you try to
        // concatenation with the `+` like this:
        // console.log("received", msg + "here");
        // `TypeError: Cannot convert object to primitive value`
        // Note that simple `console.log(msg)` is fine.
        if (message.hasOwnProperty('toString')) {
            console.warn('not suppose to have `toString` in payload, which likely trying to crash the server', message.toString);
            return;
        }
        let parsedMessage;
        try {
            parsedMessage = JSON.parse(message);
        } catch (e) {
            parsedMessage = message;
        }

        for (const subId of subscribers) {
            const listener = this.subscriptionMap[subId][1];
            listener(parsedMessage);
        }
    }
}
