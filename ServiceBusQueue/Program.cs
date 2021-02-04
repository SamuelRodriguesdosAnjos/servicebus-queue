using Azure.Messaging.ServiceBus;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace ServiceBusQueue
{
    class Program
    {
        static string connectionString = "<INSIRA AQUI A CONNECTIONSTRING>";
        static string queueName = "<INSIRA AQUI O NOME DA FILA>";

        static async Task Main()
        {
            await SendMessageAsync();

            await SendMessageBatchAsync();

            await ReceiveMessagesAsync();
        }

        static Queue<ServiceBusMessage> CreateMessages()
        {
            Queue<ServiceBusMessage> messages = new Queue<ServiceBusMessage>();

            messages.Enqueue(new ServiceBusMessage("Primeira Mensagem"));
            messages.Enqueue(new ServiceBusMessage("Segunda Mensagem"));
            messages.Enqueue(new ServiceBusMessage("Terceira Mensagem"));

            return messages;
        }

        static async Task MessageHandler(ProcessMessageEventArgs args)
        {
            string body = args.Message.Body.ToString();

            Console.WriteLine($"[Queue] Mensagem Recebida: {body}");

            await args.CompleteMessageAsync(args.Message);
        }

        static Task ErrorHandler(ProcessErrorEventArgs args)
        {
            Console.WriteLine(args.Exception.ToString());
            return Task.CompletedTask;
        }

        static async Task ReceiveMessagesAsync()
        {
            await using (ServiceBusClient client = new ServiceBusClient(connectionString))
            {
                ServiceBusProcessor processor = client.CreateProcessor(queueName, new ServiceBusProcessorOptions());

                processor.ProcessMessageAsync += MessageHandler;

                processor.ProcessErrorAsync += ErrorHandler;

                await processor.StartProcessingAsync();

                Console.WriteLine("Aguarde um minuto ou então pressione alguma tecla para sair da execução");
                Console.ReadKey();

                await processor.StopProcessingAsync();
                Console.WriteLine("Fim processamento de mensagens");
            }
        }

        static async Task SendMessageAsync()
        {
            await using (ServiceBusClient client = new ServiceBusClient(connectionString))
            {
                ServiceBusSender sender = client.CreateSender(queueName);

                ServiceBusMessage message = new ServiceBusMessage("Olá Mundo!");

                await sender.SendMessageAsync(message);

                Console.WriteLine($"Enviando uma mensagem única: {queueName}");
            }
        }

        static async Task SendMessageBatchAsync()
        {
            await using (ServiceBusClient client = new ServiceBusClient(connectionString))
            {
                ServiceBusSender sender = client.CreateSender(queueName);

                Queue<ServiceBusMessage> messages = CreateMessages();

                int messageCount = messages.Count;

                while (messages.Count > 0)
                {
                    using ServiceBusMessageBatch messageBatch = await sender.CreateMessageBatchAsync();

                    if (messageBatch.TryAddMessage(messages.Peek()))
                    {
                        messages.Dequeue();
                    }
                    else
                    {
                        throw new Exception($"Mensagem {messageCount - messages.Count} excedeu o tamanho e não puderam ser entregues.");
                    }

                    while (messages.Count > 0 && messageBatch.TryAddMessage(messages.Peek()))
                    {
                        messages.Dequeue();
                    }

                    await sender.SendMessagesAsync(messageBatch);
                }

                Console.WriteLine($"Envio de {messageCount} mensagens para o tópico: {queueName}");
            }
        }
    }
}
