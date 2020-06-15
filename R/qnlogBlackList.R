


#' 读取日志黑名单
#'
#' @param file 日志黑名单文件
#'
#' @return 返回值
#' @export
#'
#' @examples
#' qnlog_blackList_read()
qnlog_blackList_read <- function(file="data-raw/顾客问题_黑名单_reviewed.xlsx") {
  qnlog_bl <- readxl::read_excel(file,sheet = "模糊匹配")
  names(qnlog_bl) <- c('Fqn_log_blackList','Fnote')
  qnlog_bl$Fnote <- tsdo::na_replace(qnlog_bl$Fnote,'')
  class(qnlog_bl) <-'data.frame'
  return(qnlog_bl)

}
#' 读取精确匹配黑名单
#'
#' @param file 文件名
#'
#' @return 返回值
#' @export
#'
#' @examples
#' qnlog_blackListEq_read()
qnlog_blackListEq_read <- function(file="data-raw/顾客问题_黑名单_reviewed.xlsx") {
  qnlog_bl <- readxl::read_excel(file,sheet = "精确匹配")
  names(qnlog_bl) <- c('Fqn_log_blackList','Fnote')
  qnlog_bl$Fnote <- tsdo::na_replace(qnlog_bl$Fnote,'')
  class(qnlog_bl) <-'data.frame'
  return(qnlog_bl)

}

#' 上传千牛日志黑名单
#'
#' @param conn  连接
#' @param file 文件
#'
#' @return 返回值
#' @export
#'
#' @examples
#' qnlog_blackList_writeDB()
qnlog_blackList_writeDB <- function(conn=tsda::conn_rds('nsic'),file="data-raw/顾客问题_黑名单_reviewed.xlsx") {
  data <- qnlog_blackList_read(file)
  ncount <- nrow(data)
  if(ncount >0){
    #上传数据
    tsda::upload_data(conn = conn,table_name = 't_kf_blackList',data = data)

  }

}

#' 写入严格匹配黑名单到数据库
#'
#' @param conn 连接
#' @param file 文件
#'
#' @return 返回值
#' @export
#'
#' @examples
#' qnlog_blackListEq_writeDB()
qnlog_blackListEq_writeDB <- function(conn=tsda::conn_rds('nsic'),file="data-raw/顾客问题_黑名单_reviewed.xlsx") {
  data <- qnlog_blackListEq_read(file)
  ncount <- nrow(data)
  if(ncount >0){
    #上传数据
    tsda::upload_data(conn = conn,table_name = 't_kf_blackList_eq',data = data)

  }

}


#' 获取最新的黑名单
#'
#' @param conn 连接
#'
#' @return 返回值
#' @export
#'
#' @examples
#' qnlog_getBlackList()
qnlog_getBlackList <- function(conn=tsda::conn_rds('nsic')) {
  sql <- paste0("select Fqn_log_blackList  from t_kf_blackList ")
  r <- tsda::sql_select(conn,sql)
  data <- r$Fqn_log_blackList
  return(data)


}


#' 获取最新的黑名单
#'
#' @param conn 连接
#'
#' @return 返回值
#' @export
#'
#' @examples
#' qnlog_getBlackList_eq()
qnlog_getBlackList_eq <- function(conn=tsda::conn_rds('nsic')) {
  sql <- paste0("select Fqn_log_blackList  from  t_kf_blackList_eq ")
  r <- tsda::sql_select(conn,sql)
  data <- r$Fqn_log_blackList
  return(data)


}



#' 获取有效问的作者与日期数据
#'
#' @param conn 连接
#' @param FStartDate 开始日期
#' @param FEndDate 结束日期
#'
#' @return 返回值
#' @export
#'
#' @examples
#' qnlog_getUserDate()
qnlog_getUserDate <-function(conn=tsda::conn_rds('nsic'),FStartDate='2020-06-06',FEndDate='2020-06-14'){
  sql <- paste0("select distinct FUser,log_date from vw_kf_log where FIsA ='FALSE'
and log_date >='",FStartDate,"' and log_date <='",FEndDate,"'")
  data <- tsda::sql_select(conn,sql)
  class(data) <- 'data.frame'
  return(data)
}



#' 按用户按天获取日志问信息
#'
#' @param conn 连接
#' @param FUser 导购
#' @param log_date 日期
#'
#' @return 返回值
#' @export
#'
#' @examples
#' qnlog_getLog_perUserDate()
qnlog_getLog_perUserDate <- function(conn=tsda::conn_rds('nsic'),FUser='腊梅',log_date='2020-05-03'){
  sql <-paste0("select FUser,author,log_date,content from vw_kf_log where FIsA ='FALSE'
and FUser ='",FUser,"' and log_date = '",log_date,"'")
  data <- tsda::sql_select(conn,sql)
  class(data) <- 'data.frame'
  return(data)

}


#' 获取单个数据进行打款
#'
#' @param conn 连接
#' @param FUser 用户
#' @param log_date 日期
#' @param dict 字词
#'
#' @return 返回值
#' @export
#'
#' @examples
#' qnlog_logTag_read()
qnlog_logTag_read<-function(conn=tsda::conn_rds('nsic'),FUser='腊梅',log_date='2020-05-03',
                                 dict_contain=qnlog_getBlackList(),dict_eq = qnlog_getBlackList_eq()){
  data <- qnlog_getLog_perUserDate(conn,FUser,log_date)
  res <- tsdo::df_blackList_tagging(data = data,var_text = 'content',dict_contain = dict_contain,dict_equal = dict_eq)
  return(res)
}


#' 删除原有的日志信息
#'
#' @param conn 连接
#' @param FUser 用户
#' @param log_date 日志
#'
#' @return 无返回值
#' @export
#'
#' @examples
#' qnlog_logTag_delete()
qnlog_logTag_delete <- function(conn=tsda::conn_rds('nsic'),FUser='腊梅',log_date='2020-05-03') {
  sql <- paste0("delete from t_kf_logTag
where FUser ='",FUser,"' and log_date ='",log_date,"'")
  try({
    tsda::sql_update(conn,sql)
  })


}


#' 日志打标信息写入数据库
#'
#' @param conn 连接
#' @param FUser 用户
#' @param log_date 日志
#' @param dict_contain 模糊匹配
#' @param dict_eq 严格匹配
#'
#' @return 返回值
#' @export
#'
#' @examples
#' qnlog_logTag_writeDB()
qnlog_logTag_writeDB<-function(conn=tsda::conn_rds('nsic'),FUser='腊梅',log_date='2020-05-03',
                                 dict_contain=qnlog_getBlackList(),dict_eq = qnlog_getBlackList_eq()){
  data <- qnlog_logTag_read(conn,FUser,log_date,dict_contain,dict_eq)
  #fix the data type error
  data$Fflag <- as.character(data$Fflag)
  ncount <- nrow(data)
  if(ncount >0){
    #删除已有的数据
    qnlog_logTag_delete(conn,FUser,log_date)

    #写入数据
   tsda::upload_data(conn,'t_kf_logTag',data = data)
  }
}


#' 针对日志打标进行处理批量处理
#'
#' @param conn 连接
#' @param show_process 是否显示进度条
#' @param dict_contain 模糊匹配
#' @param dict_eq 精确匹配
#' @param FStartDate 开始日期
#' @param FEndDate 结束日期
#'
#' @return 返回值
#' @export
#'
#' @examples
#' qnlog_logTagBatch_writeDB()
qnlog_logTagBatch_writeDB<-function(conn=tsda::conn_rds('nsic'),show_process=FALSE,FStartDate='2020-06-08',FEndDate='2020-06-14',
                               dict_contain=qnlog_getBlackList(),dict_eq = qnlog_getBlackList_eq()){
  userDates <-qnlog_getUserDate(conn = conn,FStartDate = FStartDate,FEndDate = FEndDate)
  ncount <- nrow(userDates)
  if(ncount>0){
    if(show_process){
      #显示进度条
      withProgress(message = '日志有效问打标处理中', value = 0, {
        lapply(1:ncount, function(i){
          FUser = userDates$FUser[i]
          log_date = userDates$log_date[i]
          qnlog_logTag_writeDB(conn=conn,FUser = FUser,log_date =log_date ,dict_contain = dict_contain,dict_eq = dict_eq)
          incProgress(1/ncount, detail = paste("(",i,"/",ncount,")..."))

        })


      })

    }else{
      #不显示进度条
      lapply(1:ncount, function(i){
        FUser = userDates$FUser[i]
        log_date = userDates$log_date[i]
        qnlog_logTag_writeDB(conn=conn,FUser = FUser,log_date =log_date ,dict_contain = dict_contain,dict_eq = dict_eq)
        #incProgress(1/ncount, detail = paste("(",i,"/",ncount,")..."))

      })

    }

  }


}
