% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/future_walk.R
\name{collected_info_to_df}
\alias{collected_info_to_df}
\title{Turn a list of information about a list of calls into a data frame}
\usage{
collected_info_to_df(.l)
}
\arguments{
\item{.l}{The list of collected information}
}
\value{
a \code{tbl_df} data frame
}
\description{
The output of the \code{\link{future_walk}} functions is naturally in a list of lists. This function will turn it into a \code{tbl_df} with each column corresponding to the errors, messages, and warnings. If the list is named (i.e., the input list to \code{\link{future_walk}} was named), then there will also be a column \code{name} which will contain these values. Columns that had no returned information will default to \code{NA} and cells that did not have any returned values will default to \code{<NULL>}.
}
\seealso{
\code{\link{future_walk}}
}
