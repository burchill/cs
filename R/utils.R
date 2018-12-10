#' @export
done <- function(x) {
  future::resolved(
    eval(substitute(future::futureOf(x)),
         envir = parent.frame(1))
  )
}


#' @export
# for my own purposes in using data frames as starting parameters for jobs
iterate_over_df <- function(df, pf, f) {
  stopifnot(length(formals(f)) == 1)
  formals(f) <- alist(".dontuse" = )
  orig_bod <- body(f)
  # gives a variable .orig_row if you want to use the original row
  new_bod <- substitute({ .orig_row <- df[.dontuse, ]; with(data = df[.dontuse, ], orig_bod ) })
  body(f) <- new_bod

  pf(1:nrow(df), f)
}
