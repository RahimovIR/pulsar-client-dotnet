module Pulsar.Client.IntegrationTests.Interceptors

open System
open System.Threading
open System.Diagnostics

open System.Collections.Generic
open Expecto
open Expecto.Flip
open Pulsar.Client.Api
open FSharp.Control.Tasks.V2.ContextInsensitive
open System.Text
open System.Threading.Tasks
open Pulsar.Client.Common
open Serilog
open Pulsar.Client.IntegrationTests
open Pulsar.Client.IntegrationTests.Common


type ConsumerInterceptor() =
    
    interface IConsumerInterceptor with
        member this.Close() = ()
        member this.BeforeConsume(_, message)  =
            let prop =
                    message.Properties :> seq<_>
                    |> Seq.map(|KeyValue|)
                    |> Map.ofSeq
                    |> Map.add "BeforeConsume" "1"
            {message with Properties = prop}
        
        member this.OnAcknowledge(_,_,_) = ()
        member this.OnAcknowledgeCumulative(_,_,_) = ()
        member this.OnNegativeAcksSend(_,_,_) = ()
        member this.OnAckTimeoutSend(_,_,_) = ()

[<Tests>]
let tests =

    testList "ConsumerInterceptor" [
        testAsync "Send and receive 10 messages and check BeforeConsume" {

            Log.Debug("Started Send and receive 10 messages and check BeforeConsume")
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let numberOfMessages = 10

            let! producer =
                ProducerBuilder(client)
                    .Topic(topicName)
                    .CreateAsync() |> Async.AwaitTask
            
            let! consumer =
                ConsumerBuilder(client)
                    .Topic(topicName)
                    .ConsumerName("concurrent")
                    .SubscriptionName("test-subscription")
                    .Intercept(ConsumerInterceptor())
                    .SubscribeAsync() |> Async.AwaitTask

            let producerTask =
                Task.Run(fun () ->
                    task {
                        do! produceMessages producer numberOfMessages "concurrent"
                    }:> Task)

            let consumerTask =
                Task.Run(fun () ->
                    task {
                        let mutable acc: int = 0 
                        for i in 1..numberOfMessages do
                            let! message = consumer.ReceiveAsync()
                            let value = message.Properties.Item "BeforeConsume"
                            if value = "1" then acc <- acc + 1
                        if acc <> numberOfMessages then failwith "The number of BeforeConsume properties is not equal to numberOfMessages"
                        ()
                    }:> Task)

            do! Task.WhenAll(producerTask, consumerTask) |> Async.AwaitTask
            do! client.CloseAsync() |> Async.AwaitTask
            Log.Debug("Finished Send and receive 10 messages and check BeforeConsume")
        }
    ]