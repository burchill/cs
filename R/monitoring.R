
#' Monitor resource use on a single node
#'
#' To-do: add docs
#'
#' @export
monitor_resources_on_node <- function(username_or_command,
                                      sleeping_time = 30,
                                      total_checks = 10,
                                      command_maker = function(x) paste0("ps -u ",x," -o pcpu,rss,size,state,time,cmd")) {

  if (!is.null(command_maker))
    command = username_or_command
  else
    command = command_maker(username_or_command)

  start_up_time <- Sys.time()
  node_name <- Sys.info()[["nodename"]]
  counter = 0
  big_list = list()
  while (counter < total_checks) {
    outstring <- paste0(system(command,  intern=TRUE), collapse="\n")
    big_list <- append(big_list, outstring)
    counter = counter + 1
    Sys.sleep(sleeping_time)
  }
  return(list(time=start_up_time, nodename = node_name, data=big_list, sleeping_time=sleeping_time))
}

#' Convert monitor data into a dataframe
#'
#' To-do: add docs
#'
#' @export
resources_to_df <- function(resource_string_list,
                            original_time,
                            time_increment,
                            header="%CPU   RSS  SIZE S     TIME CMD\n",
                            numcols = 6) {
  ti <- lubridate::duration(seconds=time_increment)

  purrr::imap_dfr(
    resource_string_list,
    function(samp_string, i) {
      l <- strsplit(samp_string, "\n")[[1]]
      l <- l[2:length(l)] # remove header
      purrr::map_dfr(
        l,
        function(process) {
          proc_list <- stringr::str_split(trimws(process), "\\s+", n = numcols)[[1]]
          data.frame(t(proc_list), stringsAsFactors=FALSE)
        }) %>%
        mutate(SampleTime = original_time + i*ti)
    }) %>%
    purrr::set_names(c(stringr::str_split(trimws(header), "\\s+")[[1]], "SampleTime")) %>%
    mutate(CMD = ifelse(grepl("/usr/lib64/R/bin/exec/R", CMD), "R", CMD))
}


#' Monitor resources use on cluster
#'
#' To-do: add docs
#'
#' @export
monitor_cluster_resources <- function(login_node, node_list, save_path,
                                      sleeping_time, total_checks, ...) {
  # in case you want to just save something,
  #   this will automaticall revert to the previous plan when done
  oplan <- plan()

  # Only visit each node once
  plan(list(
    tweak(remote, workers = login_node),
    tweak(cluster, workers = unique(node_list))
  ))

  zexpr <- quote({
    furrr::future_map_dfr(
      unique(node_list),
      function (nodename) {
        resources <- monitor_resources_on_node(sleeping_time = sleeping_time,
                                               total_checks = total_checks)
        resources_to_df(resources$data,
                        resources$time,
                        resources$sleeping_time) %>%
          mutate(Nodename = nodename)
      })
  })

  if (is.null(save_path)) {
    results <- future(zexpr, substitute = FALSE)
    return(results)
  } else {
    on.exit(plan(oplan), add = TRUE)
    save_expr <- substitute({
      zplyr::collect_all(saveRDS(zexpr, save_path), catchErrors = TRUE)
    })
    results <- future(save_expr, substitute = FALSE)
    return("check it")
  }
}
