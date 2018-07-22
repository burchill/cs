#' Wrap a function with `collect_all`
#'
#' This takes a function, and returns a new one, but makes it so that it goes from f(x) to collect_all(f(x)(x)). Ehhh... look at the code.
#'
#' @param f a function
#' @export
collect_decorator <- function(f) {
  f <- purrr::as_mapper(f)
  newfie <- function() x
  formals(newfie) <- formals(f)
  bod <- body(f)
  body(newfie) <- substitute(zplyr::collect_all(bod, catchErrors = TRUE)[2:4])
  return(newfie)
}

#' Require a certain package to be installed
#'
#' Eh, just look at the code
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
#' This function is responsible for making the \code{\link{future_walk}} set of functions. The code used to make it is pretty helpful for understanding \R's meta-programming, and if you need to modify the input function `.f` to the \code{\link[furrr]{future_map}} functions, you can change the `decorator_func` argument to whatever you want.
#'
#' I wanted functions that would behave with all the complexity of `furrr`'s \code{\link[furrr]{future_map}} functions (which, due to this complexity, are mostly a blackbox to me), while modifying the behavior of the specified input function, basically so I wouldn't have to keep on adding `zplyr::collect_all` to these argument functions, like: `furrr::future_map(c(1:4), ~zplyr::collect_all(saveRDS(...)))`. I'm inordinately proud of what I did here, making a function that would be able to take \emph{any} of the `future_map` functions and turn them into what I had in mind. Ooooh lawdy, I loved learning this.
#'
#' @param .orig_function the specific `future_map`-like function you want to base yours off of
#' @param decorator_func the function that you want to use to modify the given `.f` argument I use the term "decorator" like this is analogous to Python, but it's not 100\% accurate
#' @seealso \code{\link{collect_decorator}}, \code{\link{warn_about_package}}
#' @rdname future_walk_maker
#' @export
.future_walk_maker <- function(.orig_function,
                               decorator_func = cs::collect_decorator) {
  warn_about_packages <- function() {
    warn_about_package("furrr")
    warn_about_package("zplyr",
                       " with `devtools::install_github(\"burchill/zplyr\")`.")
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
  new_bod <- substitute({ warn_about_packages(); .f <- decorator_func(.f); orig_bod })
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
#' @seealso \code{\link{collected_info_to_df}}, \code{\link{.future_walk_maker}}
#' @rdname walk_functions
#' @export
future_walk <- .future_walk_maker(furrr::future_map)
#' @rdname walk_functions
#' @export
future_walk2 <- .future_walk_maker(furrr::future_map2)
#' @rdname walk_functions
#' @export
future_iwalk <- .future_walk_maker(furrr::future_imap)
#' @rdname walk_functions
#' @export
future_pwalk <- .future_walk_maker(furrr::future_pmap)

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
