#' Immediately output warning conditions
#'
#' The function `base::warning` lets you immediately output warnings with
#' `immediate.=TRUE`, but not if the input is already a condition
#' object (weird, I know). This function is a modified Frankenstein mashup of `warning` and `message` that will immediately output warnings regardless
#' of their input form.
#' @param \dots zero or more objects which can be coerced to character (and which are pasted together with no separator) or a single condition object.
#' @param call. logical, indicating if the call should become part of the warning message.
#' @param noBreaks. logical, indicating as far as possible the message should be output as a single line when `options(warn = 1)`.
#' @param domain see `gettext`. If NA, messages will not be translated, see also the note in `stop`.
#' @export
warn_now <- function (..., call. = TRUE, noBreaks. = FALSE,
                      domain = NULL) {
  args <- list(...)
  if (length(args) == 1L && inherits(args[[1L]], "condition")) {
    cond <- args[[1L]]
    if (nargs() > 1L)
      cat(gettext("additional arguments ignored in warning()"),
          "\n", sep = "", file = stderr())
    message <- conditionMessage(cond)
    call <- conditionCall(cond)

    # What I added from `message`
    defaultHandler <- function(c) {
      cat(paste0(trimws(conditionMessage(c), "right"), "\n"), file = stderr(), sep = "")
    }
    withRestarts({
      .Internal(.signalCondition(cond, message, call))
      # and here
      defaultHandler(cond)
      # this is what I cut out
      # .Internal(.dfltWarn(message, call))
    }, muffleWarning = function() NULL)
    invisible(message)
  }
  else .Internal(warning(call., immediate.=TRUE, noBreaks., .makeMessage(...,
                                                                         domain = domain)))
}

addLF_message <- function(...) {
  args <- list(...)
  if (length(args) == 1L && inherits(args[[1L]], "condition")) {
    if (nargs() > 1L)
      warning("additional arguments ignored in message()")
    cond <- args[[1L]]
    cond$message <- paste0(
      trimws(cond$message, which="right"),
      "\n")
    message(cond)
  } else {
    message(...)
  }
}


#' Wrap a function with `collect_all`
#'
#' This takes a function, and returns a new one, but makes it so that it goes from f(x) to collect_all(f(x)(x)). Ehhh... look at the code.
#'
#' @param f a function
#' @export
collect_decorator <- function(f, indices = 1:4, asStrings = TRUE) {
  f <- purrr::as_mapper(f)
  newfie <- function() x
  formals(newfie) <- formals(f)
  bod <- body(f)
  body(newfie) <- substitute(
    zplyr::collect_all(
      bod,
      catchErrors = TRUE,
      asStrings = asStrings)[indices])
  return(newfie)
}

#' Require a certain package to be installed
#'
#' Eh, just look at the code. I kinda stopped using this, like a lazy, lazy coder.
#'
#' @param package a string of the package name
#' @param install_location a string appended to the end of the warning
#' @export
warn_about_package <- function(package, install_location=".") {
  if (!rlang::is_installed(package)) {
    m <- paste0("The `", package, "` is required for what you're doing here. Please install it", install_location)
    stop(m, call.=TRUE)
  }
}


#' Make your own versions of the `furrr` `future_map` functions
#'
#' This function is responsible for making the \code{\link{future_walk}} and \code{\link{future_cnd_map}} sets of functions. The code used to make it is pretty helpful for understanding \R's meta-programming, and if you need to modify the input function `.f` to the \code{\link[furrr]{future_map}} functions, you can change the `decorator_func` argument to whatever you want.
#'
#' I wanted functions that would behave with all the complexity of `furrr`'s \code{\link[furrr]{future_map}} functions (which, due to this complexity, are mostly a blackbox to me), while modifying the behavior of the specified input function, basically so I wouldn't have to keep on adding `zplyr::collect_all` to these argument functions, like: `furrr::future_map(c(1:4), ~zplyr::collect_all(saveRDS(...)))`. I'm inordinately proud of what I did here, making a function that would be able to take \emph{any} of the `future_map` functions and turn them into what I had in mind. Ooooh lawdy, I loved learning this.
#'
#' @param .orig_function the specific `future_map`-like function you want to base yours off of
#' @param \dots additional arguments to pass into `decorator_func`
#' @param decorator_func the function that you want to use to modify the given `.f` argument I use the term "decorator" like this is analogous to Python, but it's not 100\% accurate
#' @seealso \code{\link{collect_decorator}}, \code{\link{warn_about_package}}
#' @export
future_map_maker <- function(.orig_function,
                               ...,
                               decorator_func = cs::collect_decorator) {
  warn_about_packages <- function() {
    warn_about_package("furrr")
    warn_about_package("zplyr", " with `devtools::install_github(\"burchill/zplyr\")`.")
  }

  warn_about_package("furrr")

  # get_the_secret_shit <- function(p) {
  #   dp <- deparse(p)
  #   dp[2] <- paste0("furrr:::", gsub("^\\s+","", dp[2]))
  #   return(parse(text=dp, n=1)[[1]])
  # }

  new_function <- function() x
  formals(new_function) <- formals(.orig_function)
  # orig_bod <- get_the_secret_shit(body(.orig_function))
  orig_bod <- body(.orig_function)
  new_bod <- substitute({ warn_about_packages(); .f <- decorator_func(.f, ...); orig_bod })
  body(new_function) <- new_bod
  environment(new_function) <- asNamespace('furrr')
  return(new_function)
}



#' Supply each element in a vector to a function and collect the messages, warnings, and errors
#'
#' Think of these functions as the `walk` analogues to `furrr`'s \code{\link[furrr]{future_map} }
#' functions. However, unlike `purrr::walk` and its ilk, these functions will
#' not return the same value, but will return a list of named lists, where
#' each named list contains a sublist of messages, warnings, and errors that
#' arose during the execution of the function.  \cr \cr
#' These functions are basically designed to be used when instead of returning
#' data remotely, you want to save it somewhere. The (relatively) light-weight
#' list they'll return will help you diagnose what went wrong, etc.
#'
#' Underlyingly, these functions call the `furrr` `future_map` functions, but take the `.f` argument and essentially edit it, squeezing in a `zplyr::collect_all` wrapper while removing the `$value` element (this was done because accidentally returning huge models would slow down the parallelization considerably).
#'
#' @inheritParams furrr::future_map
#' @inheritParams furrr::future_pmap
#' @inheritParams furrr::future_map2
#'
#' @seealso \code{\link{collected_info_to_df}}, \code{\link{future_map_maker}}
#' @rdname walk_functions
#' @export
future_walk <- future_map_maker(furrr::future_map, indices=2:4)
#' @rdname walk_functions
#' @export
future_walk2 <- future_map_maker(furrr::future_map2, indices=2:4)
#' @rdname walk_functions
#' @export
future_iwalk <- future_map_maker(furrr::future_imap, indices=2:4)
#' @rdname walk_functions
#' @export
future_pwalk <- future_map_maker(furrr::future_pmap, indices=2:4)

#' Turn a list of information about a list of calls into a data frame
#'
#' The output of the \code{\link{future_walk}} functions is naturally in a list of lists. This function will turn it into a `tbl_df` with each column corresponding to the errors, messages, and warnings. If the list is named (i.e., the input list to \code{\link{future_walk}} was named), then there will also be a column `name` which will contain these values. Columns that had no returned information will default to `NA` and cells that did not have any returned values will default to `<NULL>`.
#'
#' @param .l The list of collected information
#' @return a `tbl_df` data frame
#' @seealso \code{\link{future_walk}}
#' @export
collected_info_to_df <- function(.l) {
  some_names <- !is.null(names(.l)) & (FALSE %in% (names(.l)== ""))
  df <- purrr::modify_depth(.l, 2, ~ifelse(length(.) < 1, NA, list(.))) %>%
    purrr::map_dfr(~tibble:as_tibble(.x))
  if (some_names) df %>% mutate(names = names(.l))
  else df
}



#' Evaluate conditions signalled in `future_map` calls
#'
#' These functions let you see the messages, warnings, and errors signalled in `future_map` calls. \cr \cr
#' Currently, the only conditions (here meaning: messages, warnings,
#' and errors) that are kept in non-sequential `future` plans are errors.
#' If you ran some complicated models on remote servers via `future_map`,
#' and some of these models gave you important warnings, no functions outside of the `future_map` call would ever 'know' about them. \cr \cr
#' Similar to the \code{\link{future_walk}} series of functions, the `future_cnd_map` functions will collect and preserve these conditions, but instead of converting them to strings and getting rid of the returned values, these functions will keep the conditions as S3 condition objects as well as returning the actual values in `"value"`. \cr \cr
#' `signal_fm_conditions` takes the result of these functions, signals all the conditions for each element (displaying them grouped together sequentially for easier reading), and returns the value of the map. `evaluate_fm_results` does the same thing, but doesn't signal messages and warnings.
#'
#' Like the \code{\link{future_walk}} series of functions, the `future_cnd_map` functions can be easily expanded with the \code{\link{future_map_maker}} function. You simply need to pick the `future_map` function you want to imitate and add the `asStrings = FALSE` argument.  For example, you could make a `future_cnd_map_chr` function as easily as: `future_cnd_map_chr <- future_map_maker(furrr::future_map_chr, asStrings = FALSE)`.
#'
#' @inheritParams furrr::future_map
#' @inheritParams furrr::future_pmap
#' @inheritParams furrr::future_map2
#' @param future_cnd_map_results the output of one of the `future_cnd_map`-esque functions
#' @param displayErrors whether to display all the errors that happened in the call before signalling them. Helpful in seeing *which* elements went wrong.
#' @param signalErrors whether to signal errors or ignore them. You probably should not ignore errors.
#' @examples
#' future::plan(sequential) # other plans work fine as well
#' res <- future_cnd_map(1:3, function(i) {
#'   message(i)
#'   warning("Uh oh... ", i)
#'   if (i==2)
#'     warning("Additional warning!")
#'   if (i==3)
#'     stop("OH NO!")
#'   i + 3
#' })
#' \dontrun{signal_fm_conditions(res)}
#' @seealso \code{\link{future_map_maker}}
#' @rdname condition_maps
#' @export
future_cnd_map <- future_map_maker(furrr::future_map, asStrings = FALSE)
#' @rdname condition_maps
#' @export
future_cnd_imap <- future_map_maker(furrr::future_imap, asStrings = FALSE)
#' @rdname condition_maps
#' @export
future_cnd_map2 <- future_map_maker(furrr::future_map2, asStrings = FALSE)
#' @rdname condition_maps
#' @export
future_cnd_pmap <- future_map_maker(furrr::future_pmap, asStrings = FALSE)


#' @rdname condition_maps
#' @export
evaluate_fm_results <- function(future_cnd_map_results, signalErrors = TRUE) {
  if (signalErrors == TRUE) {
    future_cnd_map_results %>%
      purrr::walk(
        function(x) {
          if (!purrr::is_empty(x[["errors"]]))
            purrr::walk(x[["errors"]], stop)
        }) %>%
      purrr::map(~.[["value"]])
  } else {
    future_cnd_map_results %>%
      purrr::map(~.[["value"]])
  }
}

#' @rdname condition_maps
#' @export
signal_fm_conditions <- function(future_cnd_map_results, displayErrors = TRUE) {
  # Messages, warnings, and errors (outputted as warnings)
  purrr::iwalk(
    future_cnd_map_results,
    function(x, i) {
      if (length(x$messages) > 0) {
        cat("Messages in .x[", i, "]:\n", file=stderr(), sep="")
        purrr::walk(x$messages, ~addLF_message(.))
      }
      if (length(x$warnings) > 0) {
        cat("Warnings in .x[", i, "]:\n", file=stderr(), sep="")
        purrr::walk(x$warnings, ~warn_now(.))
      }
      if (length(x$conditions) > 0) {
        cat("Misc. conditions in .x[", i, "]:\n", file=stderr(), sep="")
        condition_string <- x$conditions %>%
          purrr::reduce(
            function(s, cond)
              {paste0(s, trimws(cond$message, which="right"), "\n")}, .init="")
        cat(condition_string, file=stderr())
        purrr::walk(x$conditions, ~signalCondition(.))
      }
      if (length(x$errors) > 0 && displayErrors == TRUE) {
        cat("Errors in .x[", i, "]:\n", file=stderr(), sep="")
        purrr::walk(x$errors, ~cat(.$message, file=stderr(), sep=""))
      }
    }) %>%
    evaluate_fm_results()
}
#
# # future_zap(1, ~warning("AA"))
# # plan(multicore)
#
# a<-future_zap(1:3,
#               # ~warning("AAA")
#               function(x) {
#                 message("heyyo!")
#                 message("heyyo!")
#                 # message(ex2)
#                 warning("NOOO")
#                 # if (x==3)
#                 # stop(simpleError("ww"))
#                 x
#               }
# )
#
#
# plan(list(
#   tweak(remote, workers="zburchil@cycle1.cs.rochester.edu"),
#   tweak(cluster, workers=c("node92", "node91"))
# ))
#
# ba %<-% {
#   future_walk(list(1,2,list(1,2,3)),
#               function(x) {
#                 purrr::map(x[2], "A")
#               })
# }
#
#
# l2 %>% map(~.[["errors"]]) %>% compact()
#
#
# l2 <- list(list(val=1, errors=list("a","b")),
#            list(val=1, errors=list("dfda","sa")),
#            list(val=1))
# l2 %>% map(~compact(~pluck(., "errors"))
#
#
#
#
