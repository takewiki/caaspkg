#' 针对QA对进行合并
#'
#' @param conn 连接
#' @param FUser 用户
#' @param log_date 日期
#' @param sep 定义分隔符
#'
#' @return 返回日
#' @export
#'
#' @examples
#' log_qna_combined()
log_qna_combined <- function(conn=conn_rds_nsic(),FUser ='蓝淀',log_date ='2020-08-18',sep="|"){

  sql <- paste0("select FUser,log_date,FCumFlag,FIsA,author,FLog,FGroupId  from t_kf_logGroup_Input
where FUser ='",FUser,"' and log_date ='",log_date,"'
order by FCumFlag,FGroupId ")
  mydata <- tsda::sql_select(conn,sql)

  col_names <- names(mydata)
  mydata$FLag2 <- paste0(as.character(mydata$FGroupId),mydata$FIsA)
  mydata_split <- split(mydata,mydata$FLag2)
  mydata_res <- lapply(mydata_split, function(data){
    res <- data[1, ]
    info <- data$FLog
    res$FLog <- paste0(info,collapse = sep)
    return(res)
  })
  res2 <- do.call("rbind",mydata_res)
  res2 <- res2[order(res2$FGroupId),]
  res2 <- res2[ ,col_names]
  rownames(res2) <- NULL
  #删除已有数据
  sql_sel <- paste0("select 1 from t_kf_logCombined
where FUser ='",FUser,"' and log_date ='",log_date,"'")
  res_check1 <- tsda::sql_select(conn,sql_sel)
  ncount_check1 <-nrow(res_check1)
  if(ncount_check1 >0){
    #删除已有的数据
    sql_del <- paste0("delete from t_kf_logCombined
where FUser ='",FUser,"' and log_date ='",log_date,"'")
    tsda::sql_update(conn,sql_del)

  }

  #处理ID处理
  sql_maxId <- paste0("select  isnull(max(FInterId),0) as FInterId  from t_kf_logCombined")
  res_maxId <- tsda::sql_select(conn,sql_maxId)
  value_maxId <- res_maxId$FInterId
  ncount_res2 <- nrow(res2)
  res2$FInterId <- 1:ncount_res2 + value_maxId
  res2$Fflag <- ''
  res2$Ftag_by <- ''
  res2$FIs_valid <- ''
  res2$FCategory <-''

  #进行分页处理，优化性能
  ncount_res2 <- nrow(res2)
  pages <- page_setting(ncount_res2,500)
  lapply(pages, function(page){
    tsda::db_writeTable(conn,table_name = 't_kf_logCombined',r_object = res2[page,],append = T)
  })



 return(res2)

}



#' 按天获取相关的用户
#'
#' @param conn 连接
#' @param log_date 日志日期
#'
#' @return 返回值
#' @export
#'
#' @examples
#' qna_getUserListByDate()
qna_getUserListByDate <- function(conn=conn_rds_nsic(),log_date ='2020-08-19') {
  sql_sel <- paste0("select  distinct  FUser from t_kf_logGroup_Input  where log_date='",log_date,"'")
  r <- tsda::sql_select(conn,sql_sel)
  ncount <- nrow(r)
  if(ncount >0){
    res <- r$FUser
  }else{
    res<-NA
  }
  return(res)

}


#' 按天对数据进行合并
#'
#' @param conn 连接
#' @param log_date 日期
#' @param sep 分隔符
#'
#' @return 返回值
#' @export
#'
#' @examples
#' log_qna_combined_byDate()
log_qna_combined_byDate <- function(conn=conn_rds_nsic(),log_date ='2020-08-19',sep="|"){
  users <- qna_getUserListByDate(conn=conn,log_date = log_date)
  if(!is.na(users)){
    lapply(users, function(user){
      print(user)
      log_qna_combined(conn=conn,FUser = user,log_date = log_date,)
    })
  }


}

#' 针对日志数据进行打标更新
#'
#' @param conn 链接信息
#'
#' @return 返回值
#' @export
#'
#' @examples
#' log_qna_updatedLogTag()
log_qna_updatedLogTag <- function(conn=conn_rds_nsic()) {
  #更新数据
  sql_udp <-paste0("update a set a.Fflag = b.Fflag,a.Ftag_by = b.Ftag_by from t_kf_logCombined a
inner join t_kf_logTag2  b
on a.FInterId = b.FInterId")
  tsda::sql_update(conn,sql_udp)
  #删除相当的数据表
  sql_log_group <- paste0("truncate table t_kf_logGroup_Input")
  tsda::sql_update(conn=conn,sql_log_group)
  #删除打标表

}
