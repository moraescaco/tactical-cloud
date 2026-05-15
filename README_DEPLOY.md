# Tactical Cloud v1 — Deploy gratuito com PostgreSQL

Esta versão do Tactical foi preparada para rodar de duas formas:

- **Local:** se não existir `DATABASE_URL`, usa SQLite local (`mini_erp.db`).
- **Cloud:** se existir `DATABASE_URL`, usa PostgreSQL online, como Supabase ou Neon.

## Arquivos importantes

- `app.py`: sistema Tactical adaptado para detectar `DATABASE_URL`.
- `schema_postgres.sql`: estrutura das tabelas para Supabase/Neon.
- `requirements.txt`: dependências para Streamlit Cloud.
- `.env.example`: exemplo de variáveis de ambiente.
- `.gitignore`: evita subir banco local, logs e arquivos sensíveis.

## Passo rápido no Supabase

1. Crie um projeto no Supabase.
2. Vá em **Project Settings > Database > Connection string**.
3. Copie a connection string PostgreSQL.
4. No SQL Editor do Supabase, execute o conteúdo do arquivo `schema_postgres.sql`.
5. Suba esta pasta para um repositório GitHub.
6. No Streamlit Cloud, crie um app apontando para `app.py`.
7. Em **Settings > Secrets**, adicione:

```toml
DATABASE_URL = "postgresql://..."
APP_ENV = "production"
```

8. Reinicie o app.

## Login inicial

Quando o banco estiver vazio, o sistema cria automaticamente:

- Usuário: `admin`
- Senha: `admin123`

Altere a senha no primeiro acesso.

## Observação importante

Esta é uma versão de preparação cloud. Antes de usar em operação real, teste os fluxos principais:

1. Login
2. Criar produto
3. Registrar compra/entrada de estoque
4. Abrir caixa
5. Criar jogo
6. Abrir comanda
7. Adicionar item
8. Fechar comanda
9. Conferir logs/auditoria
10. Atualizar a página e confirmar que os dados persistem
