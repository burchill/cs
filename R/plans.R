# Waits until a logical expression is true or the time limit runs out
# Doesn't work for implicit futures (ie `resolved(futureOf(x))`)
wait_and_check <- function(logical_expr, total_sleep_time = 10, checks = 100) {
  expr <- rlang::enquo(logical_expr)
  counter <- 0
  while (!is.null(rlang::eval_tidy(expr)) && !rlang::eval_tidy(expr) == TRUE && counter < checks) {
    Sys.sleep(total_sleep_time/checks)
    counter <- counter + 1
  }
  return(rlang::eval_tidy(expr))
}

# Used for gathering up lists of errors from nodes, basically
message_collector <- function(l, print_function, title,
                              collect_all_name = NULL,
                              ...) {
  if (!is.null(collect_all_name)) {
    l <- purrr::map(l, ~.[[collect_all_name]]) %>%
      keep(~length(.) > 0)
  }
  if (length(l) > 0) {
    print_function(
      paste0(
        title, ":",
        reduce2(
          names(l),
          l, .init = "",
          ~ paste0(..1, "\n", ..2, ": ", paste(..3, collapse=" | ")))),
      ...)
  }
}


#' Assign futures from different levels of the topology
#'
#' Let's say you want to run a job on the bcs-cycle1 server, which
#' would require doing two nested future plans. Instead of having to
#' type out something like: \code{x \%<-\% { y \%<-\% { 1 + 1 }; y}}, you can
#' now just type \code{x \%2\% { 1 + 1 }}, which will run \code{1 + 1} on the
#' second layer of the future topology.  Likewise, \code{\%3\%} will assign
#' the expression when evaluated on the third level.
#'
#' @rdname nested_pipe
#' @export
`%2%` <- function(x, value) {
  target <- substitute(x)
  inner_expr <- substitute(value)
  expr <- substitute({.zach %<-% inner_expr; .zach })
  envir <- parent.frame(1)
  future:::futureAssignInternal(target, expr, envir = envir, substitute = FALSE)
}

#' @rdname nested_pipe
#' @export
`%3%` <- function(x, value) {
  target <- substitute(x)
  inner_expr <- substitute(value)
  expr <- substitute({.zach %<-% {.is_da_best %<-% inner_expr; .is_da_best}; .zach })
  envir <- parent.frame(1)
  future:::futureAssignInternal(target, expr, envir = envir, substitute = FALSE)
}


#' The default way to make multiprocess plans for the bcs-cycle1 server
#'
#' If one wants to run jobs on the bcs-cycle1 server.
#'
#' @param login_node a string for the gateway login, e.g., \"zachburchill@cycle1.cs.rochester.edu\"
#' @param core_function the function determining the number of cores to use on the server. By default, it is the default of `multiprocess`. You can specify a custom function or just set a number yourself
#' @param backoff_threshold the threshold of percentage of available memory below which the connection is cancelled
#' @export
bcs_planner <- function(login_node,
                        core_function = future::availableCores,
                        backoff_threshold = 0.7) {
  core_function_expr <- quo_name(enquo(core_function))

  # Make it callable if it's, say, a number
  if (!rlang::is_callable(core_function)) {
    temp_val <- core_function
    core_function <- function() return(temp_val)
  } else {
    core_function_expr <- paste0(core_function_expr, "()")
  }

  # original plan
  oplan <- plan()

  message_string <- paste0("plan(list(\n  tweak(remote, workers = \"", login_node, "\"),",
                           "\n  tweak(remote, workers = \"bcs-cycle1.cs.rochester.edu\"),",
                           "\n  tweak(multiprocess, workers = ", core_function_expr, ")\n))")
  message(paste0("Running:\n", message_string))



  plan(list(
    tweak(remote, workers = login_node),
    tweak(remote, workers = "bcs-cycle1.cs.rochester.edu"),
    tweak(multiprocess, workers = core_function())
  ))

  server_detes %<-% {
    y %<-% {
      system("free", intern = TRUE) %>%
        paste0(collapse="") %>%
        { gsub("\\s+", " ", .) } %>%
        stringr::str_match("(?<=Mem: )[0-9]+ [0-9]+") %>%
        purrr::pluck(1) %>% {
          vals <- as.numeric(strsplit(., " ")[[1]])
          1 - (vals[2]/vals[1])
        } %>%
        c(availableCores(), core_function())
    }
    y
  }
  free_mem <- server_detes[1]
  avail <- server_detes[2]
  using <- server_detes[3]
  message(paste0(round(free_mem*100), "% of memory available on server. ",
                 avail, " cores available, using: ", using))
  if (free_mem < backoff_threshold) {
    on.exit(plan(oplan), add = TRUE)
    stop(paste0("% of memory available less than ",
                round(backoff_threshold*100), ". Backing off and reverting to previous plan"))
  }
  return_null <- NULL
}


#' The default way to start a cluster plan
#'
#' Runs `future`'s plan, in a way that works well with the Rochester cluster nodes
#'
#' @param login_node a string for the gateway login, e.g., \"zachburchill@cycle1.cs.rochester.edu\"
#' @param n the number of clusters to use
#' @param \dots additional bare arguments for `filter()`
#' @param use_abbreviations because the way ssh adds known hosts, if you've only sshed into a node via `ssh nodeN`, you'll want to set this to `TRUE`
#' @return By default, nothing, but possibly an expression if `goahead=FALSE`
#' @export
default_planner <- function(login_node, n,
                            ...) {
  get_n_best_nodes(login_node, n, ...) %>%
    # Turn the nodes into the plan
    nodes_to_plan(login_node, use_abbreviations = TRUE)
}


#' Returns the n best nodes
#'
#' To do: add documentation
#'
#' @export
get_n_best_nodes <- function(login_node, n,
                             ...,
                             use_abbreviations = TRUE,
                             timeout_sec = 10) {
  stopifnot(!is.null(login_node))
  filterers <- rlang::enquos(...)

  node_df <- get_nodes_info(login_node, timeout_sec = 10)
  node_df %>%
    # Make the data frame clean
    default_cleanup() %>%
    # Filter out the ones that are down/overused
    default_filter() %>%
    # Custom filters
    filter(!!! filterers) %>%
    # Pick the n best
    pick_n_best_nodes(n)
}
#' Test nodes' connectivity
#'
#' Test and see if a node is reachable by actually trying to connect to them.
#'
#' `test_node` is for individual nodes. `test_nodes` takes a list of node names, tests each of them, and returns a list of the ones that work.
#'
#'
#' @param login_node a string for the gateway login, e.g., \"zachburchill@cycle1.cs.rochester.edu\"
#' @param timeout_sec the number of seconds to wait after trying to connect to a node before declaring it dead
#' @param .connection_timer the number of additional seconds to wait before declaring that the \emph{attempt} to connect to the node was unsuccessful
#' @param nodename a string of the node to try to connect to
#' @param node_list a list/vector of strings of node names to try to connect to
#' @param verbose a boolean on whether to print the errors, messages, and warnings that happend while testing the nodes, if any

#' @rdname testing_nodes
#' @export
test_node <- function(nodename, login_node,
                      timeout_sec, .connection_timer = 3) {
  stopifnot(!is.null(login_node))

  # if you're already on the login node, this should be NA
  if (is.na(login_node))
    da_plan <- list(tweak(remote, workers = nodename))
  # If you're not logged in already from a server:
  else {
    da_plan <- list(tweak(remote, workers = login_node), tweak(remote, workers = nodename))
    oplan <- plan()
    on.exit(plan(oplan), add = TRUE)
  }

  plan(multisession)
  # This makes sure that there's never a delay *trying* to connect (ie in `plan(da_plan)`)
  outer_test %<-% {
    plan(da_plan)
    if (is.na(login_node))
      tester %<-% { "a" }
    else
      tester %2% { "a" }
    tester_fut <- futureOf(tester)
    # This waits until tester is resolved or total_sleep_time elapses
    its_resolved <- wait_and_check(resolved(tester_fut), total_sleep_time = timeout_sec)
    if (its_resolved) tester
    else FALSE
  }
  fut <- futureOf(outer_test)
  its_resolved <- wait_and_check(resolved(fut), total_sleep_time = .connection_timer + timeout_sec)
  if (its_resolved && outer_test=="a") return(TRUE)
  else return(FALSE)
}
# # The older design
# test_node <- function(nodename, login_node, timeout_sec) {
#   stopifnot(!is.null(login_node))
#   # Sometimes you can get time-out errors
#   tryCatch(
#     {
#       plan(list(
#         tweak(remote, workers=login_node),
#         tweak(remote, workers=nodename)
#       ))
#       tester %2% { "a" }
#       Sys.sleep(timeout_sec)
#       if (resolved(futureOf(tester)) && tester=="a") return(TRUE)
#       else return(FALSE)
#     },
#     error = function(e) {
#       warning(paste0("Timeout error in node ", nodename))
#       return(FALSE)
#     }
#   )
# }

#' @rdname testing_nodes
#' @export
test_nodes <- function(node_list, login_node,
                       timeout_sec = 5,
                       verbose = FALSE,
                       .connection_timer = 3,
                       .debug = FALSE) {
  # Make sure there's a login node
  stopifnot(!is.null(login_node))

  # After we're done getting the information, revert to the original plan
  oplan <- plan()
  on.exit(plan(oplan), add = TRUE)

  # This checks to see that we can login
  plan(remote, workers = login_node)
  login_tester <- future({ "yo" })
  isitresolved <- wait_and_check(resolved(login_tester), total_sleep_time = 5)
  if ( !(isitresolved) || value(login_tester) != "yo")
    stop("Can't even log in to login node, yo")
  message("Successful login to gateway node")

  if (.debug==T) {
    print("Debug mode on. Printing out more and going slower")
    good_nodes <- c()
    for (n in node_list) {
      paste0("Testing node `", n, "`")
      test_node_value %<-% {test_node(nodename = n,
                                      login_node = NA,
                                      timeout_sec = timeout_sec,
                                      .connection_timer = .connection_timer)}
      test_node_value <- ifelse(is.null(test_node_value) | is.na(test_node_value) |  test_node_value == FALSE,
                                FALSE, TRUE)
      good_nodes <- c(good_nodes, test_node_value)
    }
    good_nodes <- purrr::set_names(good_nodes, node_list)
    print("Node values:")
    print(good_nodes)
  } else {
    # Test the nodes
    good_nodes <- future_map(
      node_list,
      function(n) {
        zplyr::collect_all({
          test_node(nodename = n,
                    login_node = NA,
                    timeout_sec = timeout_sec,
                    .connection_timer = .connection_timer)
        }, catchErrors = TRUE)
      }) %>% purrr::set_names(node_list)

    # If verbose
    if (verbose==TRUE) {
      good_nodes %>% message_collector(message, "Messages", "messages")
      good_nodes %>% message_collector(warning, "Warnings", "warnings", call.=FALSE)
      good_nodes %>% message_collector(warning, "Errors", "errors", call.=FALSE)
    }

    good_nodes <- map_lgl(
      good_nodes,
      function(n) {
        if (is.null(n$value) | is.na(n$value)) FALSE
        else n$value == TRUE })
  }

  message(paste0("Bad nodes: ", paste0(node_list[!good_nodes], collapse=", ")))
  node_list[good_nodes]
}


#' Get the status of all the nodes
#'
#' Finds the statuses of all the nodes in the cluster via logging in and
#' running `pbsnodes` on one of the nodes, and then parsing that information
#' into a data frame.
#'
#' @param login_node a string for the gateway login, e.g., \"zachburchill@cycle1.cs.rochester.edu\"
#' @param check_node the name of any "nodeN" hostnames, to get the data from. Will iterate through until it finds one that works.
#' @return A data frame with all the node information
#' @export
get_nodes_info <- function(login_node,
                           check_node=c("node64", "node33", "node34"),
                           timeout_sec = 10) {
  # After we're done getting the information, revert to the original plan
  oplan <- plan()
  on.exit(plan(oplan), add = TRUE)
  # Generally not needed, but will iterate through check nodes!
  i <- 1
  while (i <= length(check_node) && ! test_node(check_node[[i]], login_node, timeout_sec)) {
    i <- i + 1
  }

  if (i > 1) {
    if (i > length(check_node))
      stop(paste0("All 'check' nodes failed to be accessed (",
                  paste0(check_node, collapse=", "), ")"))
    message(paste0("Check nodes failed: ", paste0(check_node[1:(i-1)], collapse=", ")))
    message(paste0("Using ", check_node[[i]]))
  }
  # Change the plan
  plan(list(
    tweak(remote, workers = remote_login),
    tweak(remote, workers = check_node[[i]])
  ))

  node_text %2% { system("pbsnodes", intern = TRUE) %>% paste0(collapse="\n") }

  if (node_text=="")
    stop("Seems like the pbsnodes system is down (ie node64). Try it manually!")

  get_node_data(node_text)
}

#' Convert the output of `pbsnodes` into a list/data frame
#'
#' Runs `future`'s plan, in a way that works well with the Rochester cluster nodes
#'
#' @param info_string a single string of all the output of `pbsnodes`. Each line should be separated by a newline character
#' @param as_df if `TRUE`, it returns it as a data frame, otherwise it returns it as a list
#' @return A data frame / list with all the node information
#' @export
get_node_data <- function(info_string, as_df=TRUE) {
  node_start <- "(?<=^|\\n)node[0-9]{2}\\.cs\\.rochester\\.edu"

  node_names <- stringr::str_match_all(info_string, node_start) %>%
    purrr::simplify()
  node_info <- stringr::str_split(info_string, node_start) %>% purrr::simplify()
  # Get rid of the first empty string
  node_info <- node_info[2:length(node_info)]
  node_list <- node_info %>%
    # give each list a
    purrr::set_names(node_names) %>%
    purrr::map(function(info) {
      l <- list(state = stringr::str_match(info, "(?<=state = ).+")[1])
      nps <- list(np = stringr::str_match(info, "(?<=np = )[0-9]+")[1])
      status <- stringr::str_match(info, "(?<=status = ).+") %>%
        stringr::str_split(",") %>% {
          val_pairs <- purrr::map(., function(x) {
            stringr::str_split(x, "=")}) %>%
            purrr::pluck(1)
          purrr::map(val_pairs, 2) %>%
            purrr::set_names(purrr::map(val_pairs, 1))
        }

      if (!is.null(status) & length(status) > 1) { append(append(l, nps), status) }
      else {l}
    })
  if (as_df==TRUE) {
    # Turn it into a data frame
    imap_dfr(node_list, function(x, name) {
      as.data.frame(x, stringsAsFactors=FALSE) %>%
        mutate(node = name)
    }) %>%
      mutate(number = as.numeric(stringr::str_extract(node,"[0-9]+")))
  } else {
    node_list
  }
}



#' Default functions to help determine which cluster node to use
#'
#' These functions just apply certain default assumptions to the node information
#' data frames generated from `get_node_data`:
#'
#' * `default_cleanup` turns the important numeric values in the data frame (from `get_node_data`) from
#'   character columns to actual numeric columns, removing the "kb" from the columns pertaining to memory,
#'   and adding a column called `percent_free` which is the proportion of available memory out of the total memory.
#' * `default_filter` simply removes nodes that aren't "free," have numbers greater than 64,
#'   or have a proportion of free memory (`percent_free`) less than a certain threshold.
#' * `pick_n_best_nodes` returns `n` remaining nodes that have the most available memory.
#'
#' @param df a dataframe of node information, as produced by `get_node_data`
#' @param threshold the threshold for the proportion of memory currently available by which to exclude nodes (e.g., 0.7 excludes nodes with less than 70\% of their memory currently available)
#' @param n the number of nodes to select
#' @export
default_cleanup <- function(df) {
  df %>%
    mutate_at(vars(physmem:totmem),
              ~sub("kb", "", .)) %>%
    mutate_at(vars(loadave:nsessions), as.numeric) %>%
    {if (length(.$np) > 0)  mutate(., np = as.numeric(np))
     else . } %>%
    mutate(percent_free = availmem/totmem)
}
#' @rdname default_cleanup
#' @export
default_filter <- function(df,
                           threshold = 0.7) {
  df %>%
    filter(state=="free",
           percent_free >= threshold)
}
#' @rdname default_cleanup
#' @export
pick_n_best_nodes <- function(df, n) {
  nodes <- df %>%
    arrange(-availmem) %>%
    head(n)
  if (nrow(nodes) < n) rlang::warn(paste0("Less than ", n, " nodes available (", nrow(nodes),")"))
  nodes
}


name_if_possible <- function(x, nm = x, ...) {
  len_diff <- length(x) - length(nm)
  if (len_diff < 0) stop("`nm` cannot be LONGER than `x`")
  nm <- append(nm, rep("", len_diff))
  purrr::set_names(x, nm, ...)
}

.name_expander <- function(named_cores, ntabs=2) {
  if (rlang::is_callable(named_cores)) return ("`function()`")
  checker <- function(x) {
    ifelse(rlang::is_character(x), paste0("\"", x, "\""),
           ifelse(rlang::is_callable(x), "`function()`",
                  paste0(x)))
  }
  tabs <- function(n) paste0(rep("  ", n), collapse="")

  head <- paste0("function() {\n", tabs(ntabs), "switch(Sys.info()[[\"nodename\"]],\n")
  body <- map(named_cores, checker) %>%
    imap(function(x, i) {
      if (i == "") return(paste0(tabs(ntabs+1), x))
      else paste0(tabs(ntabs+1), "\"", i, "\" = ", x, ",")
    }) %>%
    reduce(~paste0(.x, "\n", .y))
  tail <- paste0("\n", tabs(ntabs), ")\n", tabs(ntabs-1), "}")
  paste0(head,body,tail)
}



#' Make a plan, given a set of nodes
#'
#' This is relatively complicated, because I made it needlessly flexible.
#' Basically, it will either execute `future::plan` in a topology that
#' logs into the login node, has a cluster set up with the nodes specified
#' where on each of these nodes, it uses a `multiprocess`. The needlessly flexible
#' bit is where I let the user specify the number of cores used for each cluster
#' node either by a custom function or a list corresponding to the list of cluster
#' hostnames (which can be numerics or functions).
#'
#' The bloat in this code comes from my desire to show the user what
#' command might be run. Unfortunately, it's too much of a pain in the
#'  butt to give users the proper amount of information about the
#' functions they're calling while at the same time letting them
#' customize those functions. Therefore, I represent all the arbitrary
#' custom core-setting functions with \`function()\`.
#'
#' @param df a vector/list of node hostnames or a data frame with a column called "nodes" that has the same information
#' @param login_node a string for the gateway login, e.g., \"zachburchill@cycle1.cs.rochester.edu\"
#' @param core_mapper if `NULL`, the `multiprocess` plan uses its default arguments. If a function, `core_mapper` determines the number of cores used for each machine. If a list/vector, it will be assumed to be the number of cores for each particular node, in the same order as the nodes were given. `nodes_to_plan` will then convert this into a function automatically.
#' @param goahead indicates whether this function should execute the plan or just return an expression representing the plan
#' @param use_abbreviations because the way ssh adds known hosts, if you've only sshed into a node via `ssh nodeN`, you'll want to set this to `TRUE`
#' @return Either nothing or an expression representing the structure of the plan.
#' @export
nodes_to_plan <- function(nodes, # the node df
                          login_node, # the string for the remote worker
                          core_mapper = NULL, # Not currently functional
                          goahead = TRUE, # If true, goes ahead and runs the command.
                          # If false, returns the expression
                          use_abbreviations = TRUE # will attempt to ssh into "node33" rather than "node33.cs.rochester.edu"
) {
  if (is.data.frame(nodes)) nodes <- nodes$node

  if (!is.null(core_mapper)) {
    # If core_mapper is a function
    if (rlang::is_callable(core_mapper)) {
      customWorkers <- core_mapper
      named_cores <- core_mapper
      # If core_mapper is a (named) list of core numbers/core functions
    } else if (rlang::is_list(core_mapper)) {
      named_cores <- purrr::set_names(core_mapper, nodes)
      customWorkers <- function() {
        numcores <- do.call(switch, append(Sys.info()[["nodename"]], named_cores))
        if (rlang::is_callable(numcores)) numcores()
      }
    } else {
      stop(paste0("`core_mapper` must be a function or list, not ", typeof(core_mapper)))
    }
  }

  if (use_abbreviations) nodes <- stringr::str_extract(nodes, "node[0-9]+")

  head_s <- paste0("plan(list(
                   tweak(remote, workers = \"", login_node, "\"),
                   tweak(cluster, workers = c(\"", paste0(nodes, collapse="\", \""), "\")),
                   ")
  if (is.null(core_mapper)) {
    full_s <- paste0(head_s, "multiprocess\n))")
  } else {
    full_s <-paste0(head_s, "tweak(multiprocess, workers = ", .name_expander(named_cores), ")\n))")
  }

  message(paste0("Plan being executed (minus user-defined functions):\n", full_s))

  if (goahead==TRUE) {
    plan(list(
      tweak(remote, workers = login_node),
      tweak(cluster, workers = nodes),
      multiprocess
    ))
    return_null <- NULL
  } else {
    return(rlang::parse_expr(full_s))
  }
}




