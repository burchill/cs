#' @export
done <- function(x) {
  future::resolved(
    eval(substitute(future::futureOf(x)),
         envir = parent.frame(1))
  )
}


