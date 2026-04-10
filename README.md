# CloudSEK -> IRIS Final

Versão final em PowerShell para a integração operacional CloudSEK -> IRIS.

## O que este pacote entrega

- polling configurável da CloudSEK em `/incidents/alerts`
- persistência imediata do bruto em `C:\CloudSEK\incoming`
- processamento por item
- normalização local em `C:\CloudSEK\work\normalized`
- request/response persistidos em `C:\CloudSEK\work\requests` e `C:\CloudSEK\work\responses`
- envio ao IRIS no endpoint legado validado `https://10.96.123.80/alerts/add?cid=1`
- deduplicação por `alert_source_ref`
- falhas movidas para `C:\CloudSEK\failed` com sidecar `.failure.json` e `failure-log.jsonl`
- estado local em `C:\CloudSEK\state\queue.json` e `C:\CloudSEK\state\sent-alerts.json`

## Primeira execução

1. Copie `CloudSEK.ps1` para a máquina Windows.
2. Execute uma vez:

```powershell
powershell -ExecutionPolicy Bypass -File .\CloudSEK.ps1 -RunOnce
```

3. O script criará `C:\CloudSEK\state\cloudsek-config.json` se ele ainda não existir.
4. Edite esse arquivo e preencha primeiro:
   - `CloudSEK.ApiKey`
   - `IRIS.ApiKey`
5. O pacote já sai com o mapeamento validado neste ambiente:

```json
"SeverityMap": {
  "P0": 1,
  "P1": 1,
  "P2": 4,
  "Default": 2
},
"StatusMap": {
  "open": 1,
  "acknowledged": 1,
  "in_progress": 1,
  "reopened": 1,
  "closed": 1,
  "default": 1
}
```

6. O discovery separado continua disponível para revalidação futura:

```powershell
powershell -ExecutionPolicy Bypass -File .\CloudSEK-Discover-IrisMappings.ps1
```

7. Se o discovery futuro mostrar IDs melhores ou novos estados realmente utilizados no IRIS, atualize o `cloudsek-config.json` antes de mudar a produção.

## Modos de operação

### 1. LivePoll
Consulta a CloudSEK, salva os brutos em `incoming` e processa a fila de entrada.

```powershell
powershell -ExecutionPolicy Bypass -File .\CloudSEK.ps1 -Mode LivePoll
```

### 2. PendingOnly
Processa apenas o que já estiver em `C:\CloudSEK\incoming`.

```powershell
powershell -ExecutionPolicy Bypass -File .\CloudSEK.ps1 -Mode PendingOnly -RunOnce
```

### 3. RetryFailed
Reprocessa a partir do bruto salvo em `C:\CloudSEK\failed`.

```powershell
powershell -ExecutionPolicy Bypass -File .\CloudSEK.ps1 -Mode RetryFailed -RunOnce
```

## Como validar

1. Preencha o arquivo de configuração com `CloudSEK.ApiKey` e `IRIS.ApiKey`. O mapeamento padrão já vem preenchido com os IDs validados neste ambiente.
2. Rode `LivePoll` com `-RunOnce`.
3. Confirme:
   - novos arquivos em `incoming` no momento da captura
   - arquivos normalizados em `work\normalized`
   - request e response persistidos em `work`
   - atualização de `state\queue.json`
   - atualização de `state\sent-alerts.json` quando houver sucesso
   - remoção do bruto de `incoming` após sucesso
   - movimentação para `failed` e criação do `.failure.json` em caso de erro
4. No IRIS, valide que houve criação real do alerta e que a resposta retornou `alert_id`.

## Observação importante

O script usa por padrão os IDs já validados neste ambiente:

```json
"SeverityMap": {
  "P0": 1,
  "P1": 1,
  "P2": 4,
  "Default": 2
},
"StatusMap": {
  "open": 1,
  "acknowledged": 1,
  "in_progress": 1,
  "reopened": 1,
  "closed": 1,
  "default": 1
}
```

Se o ambiente IRIS mudar no futuro, reexecute o discovery antes de alterar esses valores.


## Atualização do discovery separado

O script `CloudSEK-Discover-IrisMappings.ps1` foi mantido separado do fluxo operacional e agora:
- inclui `cid=1` e `alert_customer_id=1` na consulta ao IRIS
- tenta `Invoke-WebRequest` primeiro
- faz fallback automático para `HttpWebRequest` em casos de handshake/TLS em Windows PowerShell
- desabilita `Expect100Continue`, `KeepAlive` e revogação durante a chamada compatível
- mantém `SkipTlsValidation` respeitando a configuração

Isso não altera o endpoint legado de envio `https://10.96.123.80/alerts/add?cid=1`; apenas endurece a compatibilidade do discovery com o ambiente.
