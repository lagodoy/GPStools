#' Data cleaning
#'
#' @description \code{\link{stops_detection}} test lorem impo.
#'
#' @param input_file file with the gps data to be cleaned
#' @param output_file file to write the output of the processing
#' @param tmp_dir where to place uncompressed input file
#' @param timezone represents the timezone
#' @param speed_limit numeric which contains the speed threshold (km/h)
#' @param acceleration_limit numeric which contains the acceleration threshold(m/s^2)
#' @param date_min Select data after this date (format:YYYY-MM-AA)
#' @param date_max Select data before this date (format:YYYY-MM-AA)
#' @param cores number of cores to be used
#' @return data
#' @import dplyr
#' @importFrom data.table fread
#' @importFrom geosphere distHaversine
#' @importFrom magrittr %>%
#' @name %>%
#' @rdname pipe
#' @export
#' @examples \dontrun{
#' #do not run
#' }


stops_detection <- function(
                            in_file_gz,
                            tmp_file,
                            output_stops_file,
                            output_movement_file
                            ) {
  start_time <- Sys.time()
  library(data.table)
  library(dplyr)
  library(magrittr)
  library(R.utils)
  library(geosphere)
  library(parallel)

  if (file.exists(output_stops_file)) {
    print("Overwrite file?")
    menu_selection <- menu(c("(Y)es", "(N)o"))
    if (menu_selection == 1) {
      file.remove(output_stops_file)
    }
    else {
      return("Nothing to do.")
    }

  }

  saida <- function(x,y) {
    if (!file.exists(output_stops_file)) {
      write.table(y[[1]], output_stops_file, append = TRUE, col.names = TRUE, row.names = FALSE, quote = FALSE, sep = ',')
    }
    else
    {
      write.table(y[[1]], output_stops_file, append = TRUE, col.names = FALSE, row.names = FALSE, quote = FALSE, sep = ',')
    }

    if (!file.exists('~/Documents/MIT/MIST/Artigos/TR-E/Revisao/A.2/TripsStopsAnalysis/gdsp/Movement_4.csv')) {
      write.table(y[[2]],'~/Documents/MIT/MIST/Artigos/TR-E/Revisao/A.2/TripsStopsAnalysis/gdsp/Movement_4.csv', append = TRUE, col.names = TRUE, row.names = FALSE, quote = FALSE, sep = ',')
    }
    else
    {
      write.table(y[[2]],'~/Documents/MIT/MIST/Artigos/TR-E/Revisao/A.2/TripsStopsAnalysis/gdsp/Movement_4.csv', append = TRUE, col.names = FALSE, row.names = FALSE, quote = FALSE, sep = ',')
    }

    if (!file.exists('~/Documents/MIT/MIST/Artigos/TR-E/Revisao/A.2/TripsStopsAnalysis/gdsp/Complete_4.csv')) {
      write.table(y[[3]],'~/Documents/MIT/MIST/Artigos/TR-E/Revisao/A.2/TripsStopsAnalysis/gdsp/Complete_4.csv', append = TRUE, col.names = TRUE, row.names = FALSE, quote = FALSE, sep = ',')
    }
    else
    {
      write.table(y[[3]],'~/Documents/MIT/MIST/Artigos/TR-E/Revisao/A.2/TripsStopsAnalysis/gdsp/Complete_4.csv', append = TRUE, col.names = FALSE, row.names = FALSE, quote = FALSE, sep = ',')
    }
  }

  gunzip(in_file_gz, destname = tmp_file, overwrite = TRUE, remove = FALSE)

  dt_in_file <- fread(
    in_file_csv,
    sep = ',',
    header = TRUE
  )


  # Ids dos objetos
  object_ids <- dt_in_file %>% select(object_id) %>%
    distinct(object_id)

  n_lines_file <- nrow(dt_in_file)

  library(foreach)
  library(doParallel)
  #library(doSNOW)
  cl<-makeCluster(7)
  registerDoParallel(cl)


  oper2 <- foreach(n_object = 1:nrow(object_ids), .packages=c('dplyr', 'magrittr','R.utils', 'geosphere'),.combine = function(x,y) saida(x,y)) %dopar% {
    object <- object_ids[n_object,'object_id']

    # Inicialização de variáveis.
    count_ignicao_desligada <- 0
    count_ignicao_ligada <- 0
    count_list_movement <-  1
    count_list_stops <-  1
    delta_time_cum <- 0

    # Seleciona apenas o movimento do objeto em estudo.
    object_movement_track <- dt_in_file %>%
      filter(object_id == object) %>%
      select(object_id, lat, lon, date_time_utm, delta_time, delta_space, speed, ignicao)
    # Número de linhas do movimento do objeto.
    n_lines_movement_track <- nrow(object_movement_track)
    # Inicializa as listas para receber os dados.
    list_paradas <- data.frame(object_id=character(as.character(n_lines_movement_track)), datetime = character(n_lines_movement_track), lat=numeric(n_lines_movement_track), lon=numeric(n_lines_movement_track), delta_time_cum=numeric(n_lines_movement_track),stringsAsFactors = FALSE)
    list_movimento <- data.frame(object_id=character(n_lines_movement_track), datetime = character(n_lines_movement_track), lat=numeric(n_lines_movement_track), lon=numeric(n_lines_movement_track), delta_time=numeric(n_lines_movement_track), delta_space = numeric(n_lines_movement_track), stringsAsFactors = FALSE)
    list_complete <- data.frame(object_id=character(n_lines_movement_track), datetime = character(n_lines_movement_track), lat=numeric(n_lines_movement_track), lon=numeric(n_lines_movement_track), delta_time=numeric(n_lines_movement_track), delta_space = numeric(n_lines_movement_track), stringsAsFactors = FALSE)
    # Começa em 2 pois para avaliarmos se há uma parada precisamos medir a distância e o tempo entre, no mínimo, dois pontos.
    i <- 2

    while (i <= nrow(object_movement_track)){
      j <- 0
      init_lat <- object_movement_track[i-1,]$lat
      init_lon <- object_movement_track[i-1,]$lon
      init_datetime <- object_movement_track[i-1,]$date_time_utm
      list_pos_lat <- data.frame(lat=init_lat)
      list_pos_lon <- data.frame(lon=init_lon)
      list_pos_datetime <- data.frame(datetime = init_datetime, stringsAsFactors=FALSE)
      list_delta_time <- data.frame(delta_time = object_movement_track[i-1,]$delta_time)
      list_delta_space <- data.frame(delta_space = object_movement_track[i-1,]$delta_space)
      delta_time_cum <- 0
      desligado=NA
      if(object_movement_track[i-1,]$ignicao == 'Desligado(a)') {
        desligado=TRUE
        count_ignicao_desligada <- count_ignicao_desligada + 1
      }
      else if(object_movement_track[i-1,]$ignicao =="Ligado(a)" & is.na(desligado)){
        desligado = FALSE
        count_ignicao_ligada <- count_ignicao_ligada + 1
      }
      else if(object_movement_track[i-1,]$ignicao =="Ligado(a)" & !is.na(desligado)){
        count_ignicao_ligada <- count_ignicao_ligada + 1
      }

      while ((distHaversine (c(init_lat, init_lon), c(object_movement_track[i,]$lat,object_movement_track[i,]$lon)) <= 200) & (i <= nrow(object_movement_track))){
        if(object_movement_track[i,]$ignicao == 'Desligado(a)') {
          desligado=TRUE
          count_ignicao_desligada <- count_ignicao_desligada + 1
        }
        else if(object_movement_track[i,]== "Ligado(a)" & is.na(desligado)){
          desligado = FALSE
          count_ignicao_ligada <- count_ignicao_ligada + 1
        }
        else if(object_movement_track[i,]== "Ligado(a)" & !is.na(desligado)) {
          count_ignicao_ligada <- count_ignicao_ligada + 1
        }

        j <- j + 1
        delta_time_cum <- delta_time_cum + object_movement_track[i,]$delta_time
        list_pos_lat <- rbind(list_pos_lat, object_movement_track[i,]$lat)
        list_pos_lon <- rbind(list_pos_lon, object_movement_track[i,]$lon)
        list_pos_datetime <- rbind(list_pos_datetime, object_movement_track[i,]$date_time_utm)
        list_delta_time <- rbind(list_delta_time, object_movement_track[i,]$delta_time)
        list_delta_space <- rbind(list_delta_space, object_movement_track[i,]$delta_space)
        i <- i + 1
      }
      if ((j != 0) & (delta_time_cum >= 20*60)) {
        # Lista com as paradas.
        list_paradas[count_list_stops, 'ignicao'] <-desligado
        list_paradas[count_list_stops, 'object_id'] <- as.character(object)
        list_paradas[count_list_stops, 'datetime'] <- init_datetime
        list_paradas[count_list_stops, 'lat'] <- mean(list_pos_lat$lat)
        list_paradas[count_list_stops, 'lon'] <- mean(list_pos_lon$lon)
        list_paradas[count_list_stops, 'delta_time_cum'] <- delta_time_cum
        # Lista com trajetorias completas.
        list_complete[count_list_stops + count_list_movement-1, 'object_id'] <- as.character(object)
        list_complete[count_list_stops + count_list_movement-1, 'datetime'] <- init_datetime
        list_complete[count_list_stops + count_list_movement-1, 'lat'] <- mean(list_pos_lat$lat)
        list_complete[count_list_stops + count_list_movement-1, 'lon'] <- mean(list_pos_lon$lon)
        list_complete[count_list_stops + count_list_movement-1, 'delta_time'] <- delta_time_cum
        list_complete[count_list_stops + count_list_movement-1,  'delta_space'] <- -1
        count_list_stops <- count_list_stops + 1
      }
      else if (j != 0)
      {
        for (k in 1:(j+1)) {
          # Lista com os pontos de movimento.
          list_movimento[count_list_movement+k-1, 'object_id'] <- object
          list_movimento[count_list_movement+k-1, 'datetime'] <- list_pos_datetime$datetime[k]
          list_movimento[count_list_movement+k-1, 'lat'] <- list_pos_lat$lat[k]
          list_movimento[count_list_movement+k-1, 'lon'] <- list_pos_lon$lon[k]
          list_movimento[count_list_movement+k-1, 'delta_time'] <- list_delta_time$delta_time[k]
          list_movimento[count_list_movement+k-1, 'delta_space'] <- list_delta_space$delta_space[k]
          # Lista com trajetorias completas.
          list_complete[count_list_stops + count_list_movement+k-2, 'object_id'] <- object
          list_complete[count_list_stops + count_list_movement+k-2, 'datetime'] <- list_pos_datetime$datetime[k]
          list_complete[count_list_stops + count_list_movement+k-2, 'lat'] <- list_pos_lat$lat[k]
          list_complete[count_list_stops + count_list_movement+k-2, 'lon'] <- list_pos_lon$lon[k]
          list_complete[count_list_stops + count_list_movement+k-2, 'delta_time'] <- list_delta_time$delta_time[k]
          list_complete[count_list_stops + count_list_movement+k-2, 'delta_space'] <- list_delta_space$delta_space[k]
        }
        count_list_movement <- count_list_movement + j + 1
      }
      else
      {
        # Lista com os pontos de movimento.
        list_movimento[count_list_movement, 'object_id'] <- object
        list_movimento[count_list_movement, 'datetime'] <- list_pos_datetime$datetime[1]
        list_movimento[count_list_movement, 'lat'] <- list_pos_lat$lat[1]
        list_movimento[count_list_movement, 'lon'] <- list_pos_lon$lon[1]
        list_movimento[count_list_movement, 'delta_time'] <- list_delta_time$delta_time[1]
        list_movimento[count_list_movement, 'delta_space'] <- list_delta_space$delta_space[1]
        # Lista com trajetorias completas.
        list_complete[count_list_stops + count_list_movement-1, 'object_id'] <- object
        list_complete[count_list_stops + count_list_movement-1, 'datetime'] <- list_pos_datetime$datetime[1]
        list_complete[count_list_stops + count_list_movement-1, 'lat'] <- list_pos_lat$lat[1]
        list_complete[count_list_stops + count_list_movement-1, 'lon'] <- list_pos_lon$lon[1]
        list_complete[count_list_stops + count_list_movement-1, 'delta_time'] <- list_delta_time$delta_time[1]
        list_complete[count_list_stops + count_list_movement-1, 'delta_space'] <- list_delta_space$delta_space[1]
        count_list_movement <- count_list_movement + 1
        #count_ignicao_desligada <- 0
      }
      i <- i + 1
    }

    list_movimento_saida <- head(list_movimento,n = count_list_movement - 1)
    list_paradas_saida <- head(list_paradas,n = count_list_stops - 1)
    list_complete_saida <- head(list_complete,n = count_list_stops + count_list_movement - 2)
    write.table(object,'/tmp/caminhoes_processados.csv', append = TRUE, col.names = FALSE, row.names = FALSE)

    return(list(list_paradas_saida,list_movimento_saida, list_complete_saida, count_ignicao_desligada, count_ignicao_ligada, nrow(n_lines_movement_track)))
  }


  stopImplicitCluster()

  end_time <- Sys.time()
  end_time - start_time # Tempo de execucao.
}
