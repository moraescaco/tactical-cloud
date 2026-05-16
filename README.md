# Tactical Cloud Postgres

Sistema Streamlit para controle de jogos, operadores/jogadores, comandas, estoque, despesas, relatórios e usuários.
Esta pasta foi convertida para rodar apenas com PostgreSQL.

## Rodar

```bash
python -m pip install -r requirements.txt
python -m streamlit run app.py
```

Configure `DATABASE_URL` antes de iniciar.

Primeiro acesso padrão:

- Usuário: `admin`
- Senha: `admin123`

Altere a senha padrão após entrar.

## Atualização v32

- Logo transparente no menu lateral.
- Menu com ícones mais coerentes com cada área.
- Dashboard com comandas abertas, alertas de estoque e top operadores/jogadores.
- Removidas as frases do rodapé do menu, mantendo apenas a âncora.


© 2026 Nox Sistemas. Todos os direitos reservados.
