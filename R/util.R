#' 针对写入数据进行分页处理
#'
#' @param totalRow 总行数
#' @param pageRow  每页数
#'
#' @return 返回值
#' @export
#'
#' @examples
#' page_setting()
page_setting <- function(totalRow=14,pageRow=4) {
  if (totalRow <= pageRow){
    #如果小于分页数，一次写入
    res <- list(1:totalRow)

  }else{
    #如果大于分页数，进行分页处理
    page_count <- totalRow %/% pageRow
    #取模数
    tail_row <- totalRow %% pageRow
    #取余数
    tail_count <- 0
    if(tail_row >0){
      tail_count <- 1

    }
    total_count <-page_count+tail_count
    res <- tsdo::list_init(total_count)
    for (i in 1:page_count) {
      res[[i]] <- 1:pageRow+(i-1)*pageRow

    }

    if(tail_count >0){
      res[[total_count]] <- 1:tail_row + pageRow*page_count
    }

  }


  return(res)
}
