#' DAta clneaning title
#'
#' @description \code{\link{clean_raw_data}} test lorem impo.
#'
#' @param input_file numeric vector for passenger cars
#' @param output_file numeric vector for light commercial vehicles
#' @param timezone numeric vector for heavy good vehicles or trucks
#' @param speed_limit numeric vector for bus
#' @param acceleration_limit numeric vector for motorcycles
#' @return data
#' @importFrom data.table fread
#' @importFrom magrittr %>%
#' @importFrom geosphere distHaversine
#' @import data.table
#' @export
#' @examples \dontrun{
#' #do not run
#' }
clean_raw_data <- function(input_file,
                           output_file,
                           timezone = "Etc/UTC",
                           speed_limit = 110.0,
                           acceleration_limit = 10.0) {
  start_time <- Sys.time()
  library(data.table)
  library(dplyr)
  library(parallel)
  library(R.utils)

  saida <- function(x,y) {
    #if (!file.exists('/tmp/gdsp_Clean.csv')) {
    if (!file.exists(output_file)) {
      write.table(
        y,
        file = output_file,
        #file = '/tmp/gdsp_Clean.csv',
        append = TRUE,
        quote = FALSE,
        sep = ',',
        na = '',
        col.names = TRUE,
        row.names = FALSE
      )
    }
    else
    {
      write.table(
        y,
        file = output_file,
        #file = '/tmp/gdsp_Clean.csv',
        append = TRUE,
        quote = FALSE,
        sep = ',',
        na = '',
        col.names = FALSE,
        row.names = FALSE
      )
    }
  }


  # Data Input File Format: object_id, time, latitude, longitude.

  #in_file_gz <- '/home/leonardo/Documents/MIT/MIST/Artigos/TR-E/Revisao/A.1/Original/gdsp.csv.gz'
  in_file_gz <- input_file
  #in_file_gz <- '/dados_brutos/MISTI/Maplink/viagens_capitais_por_tipo_veiculo/recife_Truck.csv.gz'

  in_file_csv <- '/tmp/gps.csv'
  gunzip(in_file_gz, destname = in_file_csv, overwrite = TRUE, remove = FALSE)

  # Leitura das colunas veiculo, hora_coleta, lat, lon do arquivo.
  # Se header pula a primeira linha, não pula caso contrário.
  dt_in_gps_data <- data.table::fread (
    input = in_file_csv,
    sep = ',',
    header = TRUE,
    na.strings = "NA",
    #select = c("object_id", "tipo","hora_coleta", "lat", "lon"),
    select = c("object_id","time", "lat", "lon"),
    stringsAsFactors = FALSE
  )

  # Prepara a coluna com a data e hora, originalmente String, da coleta corretamente no UTM -3 e no tipo de dado PosixCT.
  date_time_utm <- as.POSIXct(
    dt_in_gps_data$time,
    format = "%Y-%m-%d %H:%M:%S",
    tz = "Etc/UTC" )
  # Timezone ajustado para UTM -3.
  attr(date_time_utm, "tzone") <- timezone

  # Anexa a coluna de tempo de coleta no formato de dado PosixCT e no fuso desejado.
  dt_in_gps_data <- data.table::data.table(dt_in_gps_data, date_time_utm)
  #dt_in_gps_data <- dt_in_gps_data %>% select(veiculo, tipo, lat, lon, date_time_utm)
  dt_in_gps_data <- dt_in_gps_data %>% dplyr::select(object_id, lat, lon, date_time_utm)
  rm(date_time_utm)
  # Precisa retirar
  dt_in_gps_data <- dt_in_gps_data %>%
    filter(date_time_utm > "2014-10-06" & date_time_utm < "2014-10-11") %>%
    select(object_id, date_time_utm, lat, lon)

  # o arrange e necessario para posterior calculo dos deltas tempo e espaço
  dt_distinct_gps_data <- dt_in_gps_data %>%
    dplyr::arrange(object_id, date_time_utm) %>%
    dplyr::distinct(object_id, date_time_utm, lat, lon, .keep_all = TRUE) %>%
    dplyr::select(object_id, date_time_utm, lat, lon)

  rm(dt_in_gps_data)

  # A partir daqui somente se parametrizado para fazer o cleaning pela velocidade
  # Calcula o delta tempo entre as coletas de cada veículo, no caso deve-se escolher nas repetições
  # de lat-lon os que
  # Ids dos objetos
  object_ids <- dt_distinct_gps_data %>% select(object_id) %>%
    distinct(object_id)

  library(foreach)
  library(doParallel)
  #library(doSNOW)
  cl<-makeCluster(7)
  registerDoParallel(cl)


  oper2 <- foreach(n_object = 1:nrow(object_ids), .packages=c('data.table','dplyr', 'magrittr','R.utils', 'geosphere'),.combine = function(x,y) saida(x,y)) %dopar% {
    object <- object_ids[n_object,'object_id']

    object_movement_track <- dt_distinct_gps_data %>%
      filter(object_id == object) %>%
      select(object_id, lat, lon, date_time_utm)

    n_lines_movement_track <- nrow(object_movement_track)

    #list_movement_track <- data.frame(object_id=character(n_lines_movement_track), lat=numeric(n_lines_movement_track), lon=numeric(n_lines_movement_track), datetime = character(n_lines_movement_track), delta_time=numeric(n_lines_movement_track), delta_space = numeric(n_lines_movement_track), stringsAsFactors = FALSE)

    dt_lag_gps_data <- object_movement_track %>%
      #group_by(veiculo) %>%
      mutate(previous_time=lag(date_time_utm), previous_lat=lag(lat), previous_lon=lag(lon))

    rm(object_movement_track)

    dt_gps_data <- dt_lag_gps_data %>%
      mutate(delta_time = as.numeric(difftime(date_time_utm, previous_time, units="secs")))

    rm(dt_lag_gps_data)

    # Cria dataframe vazio para receber as âncias percorridas entre pontos.
    dt_delta_space <- data.frame(delta_space = numeric())

    x <- 1
    f1 <- function(lat, lon, lag_lat, lag_lon) {
      function() {
        print(x)
        x <<- x + 1
        if (is.na(lag_lat)) {
          NA
        }
        else {
          geosphere::distHaversine (c(as.numeric(lag_lat), as.numeric(lag_lon)), c(as.numeric(lat),as.numeric(lon)))
        }
      }
    }

    # Calcula a distância percorrida
    # Utiliza o apply que é mais rápido que loop no R.
    delta_space<- apply( dt_gps_data,1, function(dt_gps_data) f1(dt_gps_data['lat'],dt_gps_data['lon'],dt_gps_data['previous_lat'],dt_gps_data['previous_lon'])())
    dt_delta_space<-data.frame(delta_space)
    rm(delta_space)
    rm(x)

    # Ajusta o nome do dataframe com o espaço percorrido.
    colnames(dt_delta_space) <- c("delta_space")

    dt_gps_data <- data.table(dt_gps_data, dt_delta_space)

    rm(dt_delta_space)

    # Calcula a velocidade média que o objeto percorreu entre dois pontos em km/h.
    dt_gps_data <- dt_gps_data %>%
      mutate(speed = 3.6*(delta_space/delta_time))

    # Se aceleração ativa
    # Variação de velocidade entre dois pontos.
    dt_gps_data <- dt_gps_data %>%
      mutate(delta_speed = speed - lag(speed))

    # Calcula a aceleração média estimada entre dois pontos.
    dt_gps_data<- dt_gps_data %>%
      mutate(
        acceleration = delta_speed/delta_time)

    # Seleciona apenas veiculos com ao menos 5 pontos coletados
    dt_filtered_output <- dt_gps_data %>%
      group_by(object_id)%>%
      filter(n() >= 5)

    # Finalmente elimina dados de acordo com os parâmetros de aceleração e velocidade desejados.
    # Se nao definida
    max_accelaration = 129600
    # Se nao definido
    max_speed=110
    dt_filtered_output <- dt_filtered_output %>%
      filter(((speed <= max_speed | is.na(speed)) & (abs(acceleration) <= max_accelaration | is.na(acceleration))) | ((is.na(acceleration) & speed <= max_speed))) %>%
      select(object_id, lat, lon, date_time_utm, delta_time, delta_space, speed)
    #select(veiculo, lat, lon, date_time_utm, delta_time, delta_space, speed)

    rm(dt_gps_data)
    write.table(object,'/tmp/objetos_processados.csv', append = TRUE, col.names = FALSE, row.names = FALSE)
    # if (!file.exists('/tmp/sao_paulo_Taxi_Clean.csv')) {
    #   write.table(
    #     dt_filtered_output,
    #     #file = output_file,
    #     file = '/tmp/sao_paulo_Taxi_Clean.csv',
    #     append = TRUE,
    #     quote = FALSE,
    #     sep = ',',
    #     na = '',
    #     col.names = TRUE,
    #     row.names = FALSE
    #   )
    # }
    # else
    # {
    #   write.table(
    #     dt_filtered_output,
    #     #file = output_file,
    #     file = '/tmp/sao_paulo_Taxi_Clean.csv',
    #     append = TRUE,
    #     quote = FALSE,
    #     sep = ',',
    #     na = '',
    #     col.names = FALSE,
    #     row.names = FALSE
    #   )
    # }
    return(dt_filtered_output)
  }

  stopImplicitCluster()


  end_time <- Sys.time()
  end_time - start_time # Tempo de execucao.


  #### Fim foreach
  # write.table(
  #   oper2,
  #   #file = output_file,
  #   file = '/tmp/gdsp_Clean.csv',
  #   quote = FALSE,
  #   sep = ',',
  #   na = '',
  #   col.names = TRUE,
  #   row.names = FALSE
  # )
  # Fim DataCleaning
  ######################################################
}


timezone <- "America/Sao_Paulo"
input_file <- '/home/leonardo/Documents/MIT/MIST/Artigos/TR-E/Revisao/A.2/Original/gdsp.csv.gz'
output_file <- '/home/leonardo/Documents/MIT/MIST/Artigos/TR-E/Revisao/A.2/Clean/gdsp.csv'
clean_raw_data(input_file,
               output_file,
               "America/Sao_Paulo")
