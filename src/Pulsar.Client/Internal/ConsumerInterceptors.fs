namespace Pulsar.Client.Internal

open System
open Pulsar.Client.Api
open Pulsar.Client.Common

type ConsumerInterceptors(interceptors: IConsumerInterceptor list) =
     member this.Interceptors = interceptors
     
     member this.BeforeConsume (consumer: IConsumer) (msg: Message) =
        interceptors |> List.fold(fun acc inter -> inter.BeforeConsume(consumer, acc)) msg
        
     member this.OnAcknowledge (consumer: IConsumer)(msgId: MessageId) (result:Result<_,Exception>) =
          match result with
          | Result.Ok _ ->
               interceptors |> List.iter(fun inter -> inter.OnAcknowledge(consumer, msgId, null))
          | Result.Error exn ->
               interceptors |> List.iter(fun inter -> inter.OnAcknowledge(consumer, msgId, exn))
          result

     member this.OnAcknowledgeCumulative (consumer: IConsumer) (msgId: MessageId) (result:Result<_,Exception>) =
          match result with
          | Result.Ok _ ->
               interceptors |> List.iter(fun inter -> inter.OnAcknowledgeCumulative(consumer, msgId, null))
          | Result.Error exn ->
               interceptors |> List.iter(fun inter -> inter.OnAcknowledgeCumulative(consumer, msgId, exn))
          result

     member this.OnNegativeAcksSend (consumer: IConsumer) (msgId: MessageId) (result:Result<_,Exception>) =
          match result with
          | Result.Ok _ ->
               interceptors |> List.iter(fun inter -> inter.OnNegativeAcksSend(consumer, msgId, null))
          | Result.Error exn ->
               interceptors |> List.iter(fun inter -> inter.OnNegativeAcksSend(consumer, msgId, exn))
          result
          
     member this.OnAckTimeoutSend (consumer: IConsumer) (msgId: MessageId) (result:Result<_,Exception>) =
          match result with
          | Result.Ok _ ->
               interceptors |> List.iter(fun inter -> inter.OnAckTimeoutSend(consumer, msgId, null))
          | Result.Error exn ->
               interceptors |> List.iter(fun inter -> inter.OnAckTimeoutSend(consumer, msgId, exn))
          result