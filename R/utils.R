#' @export
done <- function(x) {
  future::resolved(future::futureOf(x, envir = parent.frame(1)))
}
