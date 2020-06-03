
#' 读取用户文件
#'
#' @param file 文件名
#'
#' @return 返回值
#' @import readxl
#' @export
#'
#' @examples
#' readUserFile()
readUserFile <- function(file="data-raw/sample_users.xlsx") {
  res <- read_excel(file,
                           col_types = c("text", "text", "text",
                                         "text", "text", "text", "text", "text",
                                         "text"))
  res <- as.data.frame(res)
  return(res)

}

#' 获取导购员名称
#'
#' @param conn  连接
#' @param app_id 程序
#'
#' @return 返回列表
#' @export
#'
#' @examples
#' getCspUserName()
getCspUserName <- function(conn=tsda::conn_rds("rdbe"),app_id='caas') {
  sql <- paste0("select Fuser  from  t_md_userRight  where FappId ='",app_id,"'   and Fpermissions ='导购'")
  data <- tsda::sql_select(conn,sql)
  ncount <- nrow(data)
  if (ncount > 0){
    item <- data$Fuser
    res <- tsdo::vect_as_list(item)
  }else{
    res <- list('无导购员')
  }
  return(res)



}



