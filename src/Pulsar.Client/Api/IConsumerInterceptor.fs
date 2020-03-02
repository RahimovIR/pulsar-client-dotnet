namespace Pulsar.Client.Api

open System
open Pulsar.Client.Common

type IConsumerInterceptor =
    
    abstract member Close: unit -> unit
    
    abstract member BeforeConsume: IConsumer * Message -> Message  

    abstract member OnAcknowledge: IConsumer * MessageId * Exception -> unit

    abstract member OnAcknowledgeCumulative: IConsumer * MessageId * Exception -> unit
    
    abstract member OnNegativeAcksSend: IConsumer * MessageId * Exception -> unit
    
    abstract member OnAckTimeoutSend: IConsumer * MessageId * Exception -> unit