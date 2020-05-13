
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



