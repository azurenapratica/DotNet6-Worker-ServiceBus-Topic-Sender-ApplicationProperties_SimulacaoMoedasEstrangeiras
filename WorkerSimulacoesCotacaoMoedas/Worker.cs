using System.Text;
using System.Text.Json;
using Azure.Messaging.ServiceBus;
using WorkerSimulacoesCotacaoMoedas.Models;

namespace WorkerSimulacoesCotacaoMoedas;

public class Worker : BackgroundService
{
    private readonly ILogger<Worker> _logger;
    private readonly IConfiguration _configuration;
    private int _MoedaAtual = 0;
    private const decimal VALOR_BASE_DOLAR = 5.05m;
    private const decimal VALOR_BASE_EURO = 5.27m;
    private const decimal VALOR_BASE_LIBRA = 6.22m;

    public Worker(ILogger<Worker> logger,
        IConfiguration configuration)
    {
        _logger = logger;
        _logger.LogInformation("Iniciando a simulacao de Cotacoes de Moedas...");
        _configuration = configuration;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        string connectionString = _configuration["AzureServiceBus:Connection"];
        string nomeTopic = _configuration["AzureServiceBus:Topic"];
        
        _logger.LogInformation($"Topic = {nomeTopic}");

        while (!stoppingToken.IsCancellationRequested)
        {
            ServiceBusClient? client = null;
            ServiceBusSender? sender = null;
            try
            {
                client = new ServiceBusClient(connectionString);
                sender = client.CreateSender(nomeTopic);

                _MoedaAtual++;
                string? siglaMoeda = null;
                decimal valorBaseMoeda = 0m;
                switch (_MoedaAtual)
                {
                    case 1:
                        siglaMoeda = "USD";
                        valorBaseMoeda = VALOR_BASE_DOLAR;
                        break;
                    case 2:
                        siglaMoeda = "EUR";
                        valorBaseMoeda = VALOR_BASE_EURO;
                        break;
                    case 3:
                        siglaMoeda = "LIB";
                        valorBaseMoeda = VALOR_BASE_LIBRA;
                        break;
                }

                var cotacao = new DadosCotacao()
                {
                    Sigla = siglaMoeda,
                    Origem = nameof(WorkerSimulacoesCotacaoMoedas),
                    Horario = DateTime.Now,
                    Valor = Math.Round(valorBaseMoeda + new Random().Next(0, 21) / 1000m, 3)
                };
                var content = JsonSerializer.Serialize(cotacao);
                var message = new ServiceBusMessage(Encoding.UTF8.GetBytes(content));
                message.ApplicationProperties["moeda"] = cotacao.Sigla;

                await sender.SendMessageAsync(message);

                _logger.LogInformation($"Dados enviados ao topico: {content}");
                if (_MoedaAtual == 3) _MoedaAtual = 0;
            }
            catch (Exception ex)
            {
                _logger.LogError($"Exceção: {ex.GetType().FullName} | " +
                                $"Mensagem: {ex.Message}");
            }
            finally
            {
                if (sender is not null && !sender.IsClosed)
                    await sender.CloseAsync();
            }

            _logger.LogInformation("Execucao concluida em: {time}", DateTimeOffset.Now);
            await Task.Delay(Convert.ToInt32(_configuration["IntervaloExecucoes"]), stoppingToken);
        }
    }
}