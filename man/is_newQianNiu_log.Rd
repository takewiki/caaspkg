% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/qianNiu.R
\name{is_newQianNiu_log}
\alias{is_newQianNiu_log}
\title{判断是否新的千牛日志}
\usage{
is_newQianNiu_log(
  conn = conn_rds_nsic(),
  FUser = "admin",
  FDate = "2020-05-01"
)
}
\arguments{
\item{conn}{连接}

\item{FUser}{用户}

\item{FDate}{日期}
}
\value{
返回是否新的日志
}
\description{
判断是否新的千牛日志
}
\examples{
is_new_QianNiu_Log()
}
