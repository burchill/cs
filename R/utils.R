#' @export
done <- function(x) {
  future::resolved(
    eval(substitute(future::futureOf(x)),
         envir = parent.frame(1))
  )
}


#' @export
# for my own purposes in using data frames as starting parameters for jobs
iterate_over_df <- function(df, f, pf = purrr::map) {
  stopifnot(length(formals(f)) == 1)
  new_function <- function() x
  formals(new_function) <- alist(".dontuse" = , ".dfdata" =)
  orig_bod <- body(f)
  new_bod <- substitute({ with(data = .dfdata[.dontuse, ], orig_bod ) })
  body(new_function) <- new_bod

  pf(1:nrow(df), function(.swiz) new_function(.swiz, df))
}
