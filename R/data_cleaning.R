#' Data cleaning
#'
#' @description \code{\link{clean_raw_data}} test lorem impo.
#'
#' @param input_file file with the gps data to be cleaned
#' @param output_file file to write the output of the processing
#' @param tmp_file where to place uncompressed input file
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
clean_raw_data <- function(input_file,
                           output_file,
                           tmp_dir,
                           timezone = "Etc/UTC",
                           speed_limit = 110.0,
                           acceleration_limit = 10.0,
                           date_min = NA,
                           date_max = NA,
                           cores=7) {
  start_time <- Sys.time()
  #library(data.table)
  #library(dplyr)
  #library(parallel)
  #library(R.utils)

  if (file.exists(output_file)) {
    print("Overwrite file?")
    menu_selection <- menu(c("(Y)es", "(N)o"))
    if (menu_selection == 1) {
      file.remove(output_file)
    }
    else {
      return("Nothing to do.")
    }

  }

  write_output <- function(x,y) {
    if (!file.exists(output_file)) {
      write.table(
        y,
        file = output_file,
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

  in_file_gz <- input_file

  in_file_csv <- tmp_dir
  R.utils::gunzip(in_file_gz, destname = in_file_csv, overwrite = TRUE, remove = FALSE)

  # Read object_id, time, lat, lon from file.
  dt_in_gps_data <- data.table::fread (
    input = in_file_csv,
    sep = ',',
    header = TRUE,
    na.strings = "NA",
    select = c("object_id","time", "lat", "lon"),
    stringsAsFactors = FALSE
  )

  # Gets the datetime column as string and turn into POSIXct format.
  date_time_utm <- as.POSIXct(
    dt_in_gps_data$time,
    format = "%Y-%m-%d %H:%M:%S",
    tz = "Etc/UTC" )
  # Set the Timezone.
  attr(date_time_utm, "tzone") <- timezone

  # Join the datetime column to dt_in_gps_data.
  dt_in_gps_data <- data.table::data.table(dt_in_gps_data, date_time_utm)
  dt_in_gps_data <- dt_in_gps_data %>%
    dplyr::select(object_id, lat, lon, date_time_utm)

  #rm(date_time_utm)
  # Select by date.
  if (!is.na(date_min) & !is.na(date_max)){
    dt_in_gps_data <- dt_in_gps_data %>%
      dplyr::filter(date_time_utm > date_min & date_time_utm < date_max) %>%
      dplyr::select(object_id, date_time_utm, lat, lon)
  }
  else if (!is.na(date_min)) {
    dt_in_gps_data <- dt_in_gps_data %>%
      filter(date_time_utm > date_min) %>%
      dplyr::select(object_id, date_time_utm, lat, lon)
  }
  else if (!is.na(date_max)){
    dt_in_gps_data <- dt_in_gps_data %>%
      filter(date_time_utm < date_max) %>%
      dplyr::select("object_id", "date_time_utm", "lat", "lon")
  }

  # o arrange e necessario para posterior calculo dos deltas tempo e espaço
  dt_distinct_gps_data <- dt_in_gps_data %>%
    dplyr::arrange(object_id, date_time_utm) %>%
    dplyr::distinct(object_id, date_time_utm, lat, lon, .keep_all = TRUE) %>%
    dplyr::select("object_id", "date_time_utm", "lat", "lon")

  rm(dt_in_gps_data)

  # A partir daqui somente se parametrizado para fazer o cleaning pela velocidade
  # Calcula o delta tempo entre as coletas de cada veículo, no caso deve-se escolher nas repetições
  # de lat-lon os que
  # Ids dos objetos
  object_ids <- dt_distinct_gps_data %>% dplyr::select(object_id) %>%
    distinct(object_id)

  library(foreach)
  library(doParallel)
  #library(doSNOW)
  cl<-makeCluster(7)
  registerDoParallel(cl)


  oper2 <- foreach(n_object = 1:nrow(object_ids), .packages=c('data.table','dplyr', 'magrittr','R.utils', 'geosphere'),.combine = function(x,y) write_output(x,y)) %dopar% {
    object <- object_ids[n_object,'object_id']

    object_movement_track <- dt_distinct_gps_data %>%
      filter(object_id == object) %>%
      dplyr::select(object_id, lat, lon, date_time_utm)

    n_lines_movement_track <- nrow(object_movement_track)

    dt_lag_gps_data <- object_movement_track %>%
      mutate(previous_time=lag(date_time_utm), previous_lat=lag(lat), previous_lon=lag(lon))

    rm(object_movement_track)

    dt_gps_data <- dt_lag_gps_data %>%
      mutate(delta_time = as.numeric(difftime(date_time_utm, previous_time, units="secs")))

    rm(dt_lag_gps_data)

    # Empty dataframe to receive distances between points.
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
          geosphere::distHaversine (c(as.numeric(lag_lat), as.numeric(lag_lon)),
                                    c(as.numeric(lat),as.numeric(lon)))
        }
      }
    }

    # Calculates the distance between two consecutive points.
    delta_space<- apply( dt_gps_data,1, function(dt_gps_data) f1(dt_gps_data['lat'],dt_gps_data['lon'],dt_gps_data['previous_lat'],dt_gps_data['previous_lon'])())
    dt_delta_space<-data.frame(delta_space)
    rm(delta_space)
    rm(x)

    # Set colname.
    colnames(dt_delta_space) <- c("delta_space")

    dt_gps_data <- data.table(dt_gps_data, dt_delta_space)

    rm(dt_delta_space)

    # Calcula a velocidade média que o objeto percorreu entre dois pontos em km/h.
    dt_gps_data <- dt_gps_data %>%
      mutate(speed = 3.6*(delta_space/delta_time))

    # Variação de velocidade entre dois pontos.
    dt_gps_data <- dt_gps_data %>%
      mutate(delta_speed = speed - lag(speed))

    # Calcula a aceleração média estimada entre dois pontos.
    dt_gps_data<- dt_gps_data %>%
      mutate(
        acceleration = delta_speed/delta_time)

    # At least five rows of data.
    dt_filtered_output <- dt_gps_data %>%
      group_by(object_id)%>%
      filter(n() >= 5)

    # Finalmente elimina dados de acordo com os parâmetros de aceleração e velocidade desejados.
    # Se nao definida
    max_accelaration = acceleration_limit*12960#129600
    # Se nao definido
    max_speed=speed_limit
    dt_filtered_output <- dt_filtered_output %>%
      filter(((speed <= max_speed | is.na(speed)) & (abs(acceleration) <= max_accelaration | is.na(acceleration))) | ((is.na(acceleration) & speed <= max_speed))) %>%
      dplyr::select("object_id", "lat", "lon", "date_time_utm", "delta_time", "delta_space", "speed")

    rm(dt_gps_data)
    write.table(object,'/tmp/objetos_processados.csv', append = TRUE, col.names = FALSE, row.names = FALSE)
    return(dt_filtered_output)
  }

  stopImplicitCluster()


  end_time <- Sys.time()
  end_time - start_time # Running time.


  #### Fim foreach
}


# timezone <- "America/Sao_Paulo"
# input_file <- '/home/leonardo/Documents/MIT/MIST/Artigos/TR-E/Revisao/A.1/Original/gdsp.csv.gz'
# output_file <- '/home/leonardo/Documents/TesteBiblioteca/teste.csv'
# tmp_file <- '/tmp/gps.csv'
#clean_raw_data(input_file,
#               output_file,
#               tmp_file,
#               speed_limit = 110.0,
#               acceleration_limit = 10.0,
#               timezone = "America/Sao_Paulo",
#               date_min="2014-10-06",
#               date_max="2014-10-11",
#               cores=7)

