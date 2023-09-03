library(sqldf)
library(dplyr)
library(lubridate)
library(ggplot2)

setwd("C:/Users/alexm/OneDrive/Desktop/Pós Data Science/3º Trimestre/Visualization")
csvs <- list.files(pattern = "\\.csv$")
list_of_dfs <- lapply(csvs, function(x) read.csv(x))

df1 <- list_of_dfs[[1]]
df2 <- list_of_dfs[[2]]
df3 <- list_of_dfs[[3]]
df4 <- list_of_dfs[[4]]
df5 <- list_of_dfs[[5]]
df6 <- list_of_dfs[[6]]
df7 <- list_of_dfs[[7]]
df8 <- list_of_dfs[[8]]
df9 <- list_of_dfs[[9]]
df10 <- list_of_dfs[[10]]

df1$order_delivered_customer_date <- as.POSIXct(df1$order_delivered_customer_date, format = "%Y-%m-%d %H:%M:%S")
df1$order_approved_at <- as.POSIXct(df1$order_approved_at, format = "%Y-%m-%d %H:%M:%S")
df1$order_delivered_carrier_date <- as.POSIXct(df1$order_delivered_carrier_date, format = "%Y-%m-%d %H:%M:%S")
df1$review_creation_date <- as.POSIXct(df1$review_creation_date, format = "%Y-%m-%d %H:%M:%S")
df1$review_answer_timestamp <- as.POSIXct(df1$review_answer_timestamp, format = "%Y-%m-%d %H:%M:%S")

# realizando a diferença de timestamps antes de usar o sqldf porque ele tem mais dificuldades do que o R em lidar com datas

df1 <- df1 |> 
  mutate(frete_tmp = as.numeric(difftime(order_delivered_customer_date, order_purchase_timestamp, units = "hours"))) |> 
  mutate(postagem_tmp = as.numeric(difftime(order_delivered_carrier_date, order_purchase_timestamp, units = "hours"))) |>
  mutate(tempo_resposta_review = as.numeric(difftime(review_answer_timestamp, review_creation_date, units = "hours")))

query1 <- sqldf("SELECT order_id, order_delivered_customer_date, review_comment_message, review_id, review_score, review_answer_timestamp, order_approved_at, order_delivered_carrier_date, review_creation_date, seller_id, frete_tmp, tempo_resposta_review, payment_sequential, payment_installments, postagem_tmp, product_id, order_item_id, price, freight_value, payment_value, payment_type,product_category_name, customer_unique_id, seller_state, customer_state, flag_atraso, flag_insatisfeito
                FROM df1 a")

por_vendedor_vendas_distintas <- query1 |> 
  group_by(seller_id, order_id) |>  
  summarise(
    mx_payment_sequential = max(ifelse(is.na(payment_sequential), 0, payment_sequential)),
    mx_payment_installments = max(ifelse(is.na(payment_installments), 0, payment_installments)),
    produtos_distintos = n_distinct(product_id)
  ) |> 
  group_by(seller_id) |>  
  summarise(
    vendas_distintas = sum(!is.na(order_id)),
    av_payment_installments = mean(mx_payment_installments, na.rm = TRUE),
    max_payment_installments = max(mx_payment_installments, na.rm = TRUE),
    av_payment_sequential = mean(mx_payment_sequential, na.rm = TRUE),
    max_payment_sequential = max(mx_payment_sequential, na.rm = TRUE)
  )

por_vendedor_frete_vlr_medio <- query1 |> 
                        group_by(seller_id) |>
                        summarise(frete_medio_vlr = mean(freight_value))

por_vendedor_postagem_tmp_medio <- query1 |> 
                        group_by(seller_id) |>
                        summarise(postagem_tmp_medio = mean(postagem_tmp))

mode_function <- function(x) {
  uniqx <- unique(x)
  uniqx[which.max(tabulate(match(x, uniqx)))]
}
  
por_vendedor_pagamento_preferido <- query1 |> 
                        group_by(seller_id) |>
                        summarise(pagament0_preferido = mode_function(payment_type))
  
por_vendedor_categoria_preferida <- query1 |> 
                        group_by(seller_id) |>
                        summarise(categoria_preferida = mode_function(product_category_name))
  
por_vendedor_vendas_outro_estado <- query1 |> 
                        group_by(seller_id) |>
                        mutate(outro_estado = ifelse(seller_state != customer_state, 1, 0)) |> 
                        summarise(contagem_outro_estado = sum(outro_estado))
  
por_vendedor_atrasos <- query1 |> 
                        group_by(seller_id) |>
                        summarize(atrasos = sum(flag_atraso))

por_vendedor_insatisfeito <- query1 |> 
                        group_by(seller_id) |>
                        summarize(insatisfeito = sum(flag_insatisfeito, na.rm = TRUE))

por_vendedor_frete_over_payment <- query1 |> 
  group_by(seller_id) |> 
  summarise(frete_over_payment = sum(freight_value) / sum(payment_value))

por_vendedor_reviews <- query1 |> 
  group_by(seller_id) |> 
  summarise(
    reviews_contagem = n_distinct(ifelse(is.na(review_id), 0, review_id)),
    review_media = mean(review_score, na.rm = TRUE),
    review_comment_message_length_avg = mean(nchar(ifelse(is.na(review_comment_message), 0, review_comment_message)), na.rm = TRUE),
    review_comment_n_vazios = sum(nchar(review_comment_message) > 0) 
  )

por_vendedor_tempo_reposta_review <- query1 |>
  group_by(seller_id) |> 
  summarise(tempo_reposta_review_mean = mean(tempo_resposta_review))

por_vendedor_tempo_frete <- query1 |>
  group_by(seller_id) |> 
  summarise(tempo_frete_medio = mean(frete_tmp),
            tempo_postagem_medio = mean(postagem_tmp),
            tempo_postagem_porcentagem_frete = (mean(postagem_tmp)/mean(frete_tmp)))

df_vendedor <- por_vendedor_tempo_frete |> 
  left_join(por_vendedor_tempo_reposta_review, by = "seller_id") |>
  left_join(por_vendedor_vendas_distintas, by = "seller_id") |>
  left_join(por_vendedor_reviews, by = "seller_id") |>
  left_join(por_vendedor_frete_over_payment, by = "seller_id") |>
  left_join(por_vendedor_insatisfeito, by = "seller_id") |>
  left_join(por_vendedor_atrasos, by = "seller_id") |>
  left_join(por_vendedor_vendas_outro_estado, by = "seller_id") |>
  left_join(por_vendedor_categoria_preferida, by = "seller_id") |>
  left_join(por_vendedor_pagamento_preferido, by = "seller_id") |>
  left_join(por_vendedor_postagem_tmp_medio, by = "seller_id") |>
  left_join(por_vendedor_frete_vlr_medio, by = "seller_id")
  
# parei por aqui por enquanto


data_final = as.POSIXct("2018-08-29 15:00:37")

vendas_vendedor_últ_ano <- df1 |> 
  filter(order_purchase_timestamp > (data_final - years(1))) |> 
  group_by(seller_id) |> 
  summarise(vendas_anuais = sum(price))

frete_over_payment_vendedor_últ_ano <- df1 |> 
  filter(order_purchase_timestamp > (data_final - years(1))) |> 
  group_by(seller_id) |> 
  summarise(frete_over_payment = sum(freight_value) / sum(payment_value))

# tem um valor acima de 1, indicando que há algum erro nos preços relativos à essa venda - única linha que não bate é do seller_id 8b181ee5518df84f18f4e1a43fe07923
# tabela útil para verificar o impacto médio do frete no valor pago total pelo cliente (payment_value), que é diferente do valor recebido pelo vendedor (price)
# o inversto de frete_over_payment é a porcentagem do valor pago pelo cliente para a compra que efetivamente vai pro vendedor

ggplot(vendas_vendedor_últ_ano, aes(x=seller_id, y=vendas_anuais, fill=seller_id)) +
  geom_bar(stat="identity") +
  labs(title="Vendas totais no ano passado por vendedor", 
       x="Vendedor", 
       y="Venda Anual") +
  theme_minimal() +
  theme(legend.position="none")

vendas_vendedor_últ_ano_ordenado <- vendas_vendedor_últ_ano |> 
  arrange(-vendas_anuais) |> 
  mutate(seller_id = factor(seller_id, levels = seller_id))

ggplot(vendas_vendedor_últ_ano_ordenado, aes(x=seller_id, y=vendas_anuais, fill=seller_id)) +
  geom_bar(stat="identity") +
  labs(title="Vendas totais no ano passado por vendedor", 
       x="Vendedor", 
       y="Venda Anual") +
  theme_minimal() +
  theme(legend.position="none")

faixas_renda <- c(-Inf, 80999.99, 359999.99, 3599999.99, Inf)
extratos <- c("MEI - até 81k", "ME - até 360k", "EPP - até 3.6kk", "Acima")

vendas_vendedor_últ_ano_ordenado$faixas <- cut(vendas_vendedor_últ_ano_ordenado$vendas_anuais, breaks = faixas_renda, labels = extratos, right = TRUE)

# vendas anuais do último ano da base por vendedor ordenado por volume das vendas em R$

ggplot(vendas_vendedor_últ_ano_ordenado, aes(x=reorder(seller_id, vendas_anuais), y = vendas_anuais, fill=faixas)) +
  geom_bar(stat="identity") +
  labs(title="Vendas totais no ano passado por vendedor",
       y="Venda Anual", 
       fill = "Faixas") +
  theme_minimal() +
  theme(legend.position="none")

# mesmo gráfico, mas com eixo y em log

ggplot(vendas_vendedor_últ_ano_ordenado, aes(x=reorder(seller_id, vendas_anuais), y = vendas_anuais, fill=faixas)) +
  geom_bar(stat="identity") +
  scale_y_logy10()
  labs(title="Vendas totais no ano passado por vendedor",
       y="Venda Anual", 
       fill = "Faixas") +
  theme_minimal() +
  theme(legend.position="none")

  
  faixas_renda <- c(-Inf, 80999.99, 359999.99, 3599999.99, Inf)
  extratos <- c("MEI - até 81k", "ME - até 360k", "EPP - até 3.6kk", "Acima")  
  
vendas_por_faixa <- vendas_vendedor_últ_ano_ordenado |> 
  group_by(faixas) |> 
  summarise(vendas_anuais2 = sum(vendas_anuais),
                                 contagem = n())

# vendas anuais do último ano da base por faixa de renda dada pelo Sebrae, em R$

ggplot(vendas_por_faixa, aes(x=faixas, y = vendas_anuais2, fill=faixas)) +
  geom_bar(stat="identity") +
  geom_text(aes(label = contagem), vjust = -0.5) +  
  labs(title="Vendas totais no ano passado agregadas por extrato vendedor", 
       x="Extrato", 
       y="Venda Anual", 
       fill = "Faixas") +
  theme_minimal() +
  theme(legend.position="none", axis.text.x = element_text(angle = 45, hjust = 1))
