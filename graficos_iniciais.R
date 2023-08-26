install.packages(sqldf)
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

query1 <- sqldf("SELECT seller_id, product_id, order_item_id, freight_value, payment_type,product_category_name, customer_unique_id, seller_state, customer_state, flag_atraso, flag_insatisfeito
                FROM df1 a")

por_vendedor_vendas_distintas <- query1 |> 
                        group_by(seller_id) |> 
                        summarise(vendas_distintas = sum(!is.na(product_id)))
  
por_vendedor_produtos <- query1 |> 
                        group_by(seller_id) |> 
                        summarise(produtos = n_distinct(product_id))

por_vendedor_frete_medio <- query1 |> 
                        group_by(seller_id) |>
                        summarise(frete_medio = mean(freight_value))

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

df_vendedor <- por_vendedor_vendas_distintas |> 
  left_join(por_vendedor_produtos, by = "seller_id") |>
  left_join(por_vendedor_frete_medio, by = "seller_id") |>  
  left_join(por_vendedor_pagamento_preferido, by = "seller_id") |> 
  left_join(por_vendedor_categoria_preferida, by = "seller_id") |> 
  left_join(por_vendedor_vendas_outro_estado, by = "seller_id") |> 
  left_join(por_vendedor_atrasos, by = "seller_id") |>
  left_join(por_vendedor_insatisfeito, by = "seller_id")

data_final = as.POSIXct("2018-08-29 15:00:37")

vendas_vendedor_últ_ano <- df1 |> 
  filter(order_purchase_timestamp > (data_final - years(1))) |> 
  group_by(seller_id) |> 
  summarise(vendas_anuais = sum(price))

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

ggplot(vendas_vendedor_últ_ano_ordenado, aes(x=reorder(seller_id, vendas_anuais), y = vendas_anuais, fill=faixas)) +
  geom_bar(stat="identity") +
  labs(title="Vendas totais no ano passado por vendedor",
       y="Venda Anual", 
       fill = "Faixas") +
  theme_minimal() +
  theme(legend.position="none")

vendas_por_faixa <- vendas_vendedor_últ_ano_ordenado |> 
  group_by(faixas) |> 
  summarise(vendas_anuais2 = sum(vendas_anuais))

ggplot(vendas_por_faixa, aes(x=faixas, y = vendas_anuais2, fill=faixas)) +
  geom_bar(stat="identity") +
  labs(title="Vendas totais no ano passado agregadas por extrato vendedor", 
       x="Extrato", 
       y="Venda Anual", 
       fill = "Faixas") +
  theme_minimal() +
  theme(legend.position="none", axis.text.x = element_text(angle = 45, hjust = 1))

ggplot(vendas_por_faixa, aes(x=faixas, y = vendas_anuais2, fill=faixas)) +
  geom_bar(stat="identity") +
  geom_text(aes(label=scales::percent(proportion, accuracy = 0.1), y = vendas_anuais), 
             vjust=-0.5, size = 4) +
  labs(title="Vendas totais no ano passado agregadas por extrato vendedor", 
       x="Extrato", 
       y="Venda Anual", 
       fill = "Faixas") +
  theme_minimal() +
  theme(legend.position="none", axis.text.x = element_text(angle = 45, hjust = 1))
