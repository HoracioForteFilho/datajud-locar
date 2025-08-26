# DataJud Locar Pipeline

Este repositório contém um pipeline em Python para coletar e consolidar
informações de processos judiciais associados a um nome e documento
(CNPJ/CPF) nos tribunais brasileiros integrados à API pública do CNJ.

## Recursos

- **Coleta automatizada** via API pública do CNJ com tratamento de erros e
  tentativas com backoff para lidar com rate limiting (429) e falhas
  temporárias.
- **Extração de indicadores jurídicos**: prazos relevantes, resumo da
  última decisão e sinalização de fase de execução a partir das
  descrições das movimentações do processo.
- **Exportação de dados** em múltiplos formatos (Excel, CSV, SQLite e
  PDF ou TXT) configuráveis por linha de comando.
- **Filtro temporal** por data mínima (`--desde`) ou número de dias a
  partir da data atual (`--since-days`).
- **Modo de auto‑teste** (`--selftest`) para validar as rotinas de
  exportação sem necessidade de acesso à API.

## Uso rápido

1. Instale as dependências básicas:
   ```bash
   pip install requests pandas openpyxl
   ```
   Para gerar PDF, instale também `fpdf2`:
   ```bash
   pip install fpdf2
   ```

2. Defina a chave de API do CNJ (obtida em
   <https://www.cnj.jus.br/sgdcnj/registro-sistema-anali>), por exemplo:
   ```bash
   export DATAJUD_API_KEY="sua_chave_secreta"
   ```

3. Execute o pipeline passando os parâmetros desejados, por exemplo:
   ```bash
   python datajud_locar_pipeline_v2.py \
     --nome "LOCAR SANEAMENTO AMBIENTAL LTDA" \
     --cnpj 35474949000108 \
     --tribunais tjpe tjba tjsp trf5 \
     --max-paginas 25 \
     --excel processos_locar.xlsx \
     --pdf processos_locar.pdf \
     --sqlite processos_locar.db \
     --csv processos_locar.csv
   ```

4. Para testar a exportação sem fazer chamadas à API, utilize o modo de auto‑teste:
   ```bash
   python datajud_locar_pipeline_v2.py --selftest \
     --excel demo.xlsx --pdf demo.pdf --sqlite demo.db --csv demo.csv
   ```

## Workflow GitHub Actions

Este repositório inclui um workflow (`.github/workflows/datajud.yml`) que
permite disparar a coleta via **GitHub Actions** e publicar os arquivos
gerados em GitHub Pages. Para utilizá‑lo:

1. Crie um secret no repositório com o nome `DATAJUD_API_KEY` contendo
   sua chave de API.
2. Habilite GitHub Pages via **Settings → Pages** configurando a fonte
   "GitHub Actions".
3. Acesse a aba **Actions**, escolha o workflow **DataJud Run** e clique
   em “Run workflow”. Informe os parâmetros solicitados (nome, cnpj,
   tribunais e max_paginas) e aguarde a conclusão.
4. Os artefatos (Excel, PDF/TXT, SQLite e CSV) estarão disponíveis
   para download na execução do workflow e também serão publicados em
   `https://<seu-usuario>.github.io/<seu-repositorio>/`.

## .gitignore

O arquivo `.gitignore` padrão deste repositório exclui arquivos
temporários, ambientes virtuais e dados sensíveis. Ajuste‑o conforme
necessário para o seu fluxo de trabalho.

## Licença

Este projeto é disponibilizado sem garantias; utilize por sua conta e
risco. Adapte conforme as necessidades da sua análise jurídica.
