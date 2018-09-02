check_multisession_hierarchy <- function() {
  oplan <- plan("list")
  if (!("multisession" %in% attr(oplan[[1]], "class")))
    stop("`multisession` needs to be the top plan")
  if (length(oplan) < 2)
    stop("The plan needs to be more than one level for embedding")
}

#' Beep when future is resolved
#'
#' This is pretty untested code, but the gist is that it will make a beeping sound when the future is resolved. In the meantime however, you can't change the `plan` and the current plan needs to have a `multisession` layer at the top of the hierachy, with the layer(s) you want to work on below it. This function also requires `beepr` to be installed. \cr
#' The `futureBeep` function is basically a wrapper of `future()` (and lets you specify the beep sound), and \%beep\% is basically a wrapper of \%<-\%. \cr
#' You can change the default beep sound with options("default.beepsound"). \cr
#' To-do: add docs
#'
#' @rdname beeper
#' @export
`%beep%` <- function (x, value) {
  check_multisession_hierarchy()
  .beepsound <- getOption("default.beepsound", 1)

  target <- substitute(x)
  inner_expr <- substitute(value)
  expr <- substitute({
    .zach %<-% inner_expr
    while (!resolved(futureOf(.zach))) Sys.sleep(2)
    beepr::beep(.beepsound)
    .zach
  })
  envir <- parent.frame(1)
  future:::futureAssignInternal(target, expr, envir = envir,
                                substitute = FALSE)
}

#' @rdname beeper
#' @export
futureBeep <- function (expr, envir = parent.frame(), substitute = TRUE, globals = TRUE,
                        packages = NULL, lazy = FALSE, seed = NULL, evaluator = plan("next"),
                        ...,
                        .beep_sound = getOption("default.beepsound", 1),
                        .sleep_interval = 3) {
  check_multisession_hierarchy()

  if (substitute)
    expr <- substitute(expr)
  big_expr <- substitute({
    .qua <- future::future(expr, envir, substitute, globals, packages, lazy, seed, evaluator, ...)
    while(!resolved(.qua)) Sys.sleep(.sleep_interval)
    beepr::beep(.beep_sound)
    value(.qua)
  })
  if (!is.function(evaluator)) {
    stop("Argument 'evaluator' must be a function: ", typeof(evaluator))
  }
  future <- evaluator(big_expr, envir = envir, substitute = FALSE,
                      lazy = lazy, seed = seed, globals = globals, packages = packages,
                      ...)
  if (!inherits(future, "Future")) {
    stop("Argument 'evaluator' specifies a function that does not return a Future object: ",
         paste(sQuote(class(future)), collapse = ", "))
  }
  future
}


#' Kill R processes on nodes
#'
#' This command requires a "secondary" login node, i.e., a different gateway node to access the cluster nodes.  Then it just uses `pidof R` and `kill` UNIX commands to kill R. Probably only works on UNIX machines. \cr
#' To-do: add docs
#'
#' @export
kill_r_on_nodes <- function(node_list, secondary_login_node,
                            wait_until_resolved = FALSE,
                            beep_on_end = FALSE) {
  if (beep_on_end==TRUE)
    stopifnot(rlang::is_installed("beepr"))
  # Can't have this if we wan't to return a future
  if (wait_until_resolved==TRUE) {
    oplan <- plan()
    on.exit(plan(oplan), add = TRUE)
  }
  plan(list(tweak(remote, workers = secondary_login_node),
            tweak(cluster, workers = unique(node_list))))

  killer <- future({
    xxx <- future_map(c(1:length(unique(node_list))),
                      function(x) { names <- system("pidof R", intern=TRUE); system(paste0("kill ", names)); "done"})
    xxx})
  if (wait_until_resolved==TRUE) {
    while (!resolved(killer)) Sys.sleep(1)
    beepr::beep()
    return(value(killer))
  } else return(killer)
}







#' Monitor resource use on a single node
#'
#' To-do: add docs
#'
#' @export
monitor_resources_on_node <- function(username_or_command,
                                      sleeping_time = 30,
                                      total_checks = 10,
                                      command_maker = function(x) paste0("ps -u ",x," -o pcpu,rss,size,state,time,cmd")) {

  if (is.null(command_maker))
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
monitor_cluster_resources <- function(username_or_command,
                                      login_node, node_list, save_path,
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
        resources <- monitor_resources_on_node(username_or_command,
                                               sleeping_time = sleeping_time,
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
