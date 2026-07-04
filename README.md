# Projetando Fluxos de Trabalho com Prefect

## Padrão de TIMEZONE para usar

> America/Sao_Paulo

## Como fazer Deployment do workflow

```bash
prefect deployment run '<flow_name>/<deployment_name>'
```

> O `<flow_name>` é colocado no `@flow` decorator.

> O `<deployment_name>` é colocado no `.serve` ao final.
- Caso não seja: _The name to give the created deployment. Defaults to the name of the flow._

---

## Games Discounts Bot

> Useful links:
- [User-Agents](https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/User-Agent)
- [Games Free API](https://apidocs.cheapshark.com/#b9b738bf-2916-2a13-e40d-d05bccdce2ba)

