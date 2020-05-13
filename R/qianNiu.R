#' 查询客服报表
#'
#' @param FStartDate 开始日期
#' @param FEndDate  结束日期
#' @param conn 连接
#'
#' @return 返回数据
#' @import tsda
#' @export
#'
#' @examples
#' getCspRpt()
getQianNiuRpt <- function(conn=conn_rds_nsic(),FStartDate='2020-04-28',FEndDate='2020-05-07'){
  sql <- paste0("select FLog  as 日志原文,log_datetime  as 日期时间,log_date as 日期,log_time  as 时间,author  as 对象 ,content  as 会话,FUser 用户,FUploadDate 上传日志,FIsA  品牌方 from t_kf_log
where log_date >= '",FStartDate,"' and log_date <='",FEndDate,"'")
  r <- tsda::sql_select(conn,sql)
  return(r)

}


#' 查询客服报表
#'
#' @param FStartDate 开始日期
#' @param FEndDate  结束日期
#' @param conn 连接
#'
#' @return 返回数据
#' @import tsda
#' @export
#'
#' @examples
#' getCspRpt()
getQianNiu2Rpt <- function(conn=conn_rds_nsic(),FStartDate='2020-04-28',FEndDate='2020-05-07'){
  sql <- paste0("select FUser 导购,FDate 日志日期,FSessionCount 会话数    from t_kf_logStat
where FDate >= '",FStartDate,"' and FDate <='",FEndDate,"'")
  r <- tsda::sql_select(conn,sql)
  return(r)

}

#' 判断是否新的千牛日志
#'
#' @param conn 连接
#' @param FUser 用户
#' @param FDate 日期
#'
#' @return 返回是否新的日志
#' @export
#'
#' @examples
#' is_new_QianNiu_Log()
is_newQianNiu_log <- function(conn=conn_rds_nsic(),FUser='admin',FDate='2020-05-01') {
  sql <- paste0("select top 1 * from t_ic_statALL
where FUser ='",FUser,"'and FDate='",FDate,"'")
  r<- tsda::sql_select(conn,sql)
  ncount <- nrow(r)
  if(ncount ==0 ){
    res <- TRUE
  }else{
    res <- FALSE
  }

  return(res)


}






