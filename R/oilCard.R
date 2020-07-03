#' 查询油卡
#'
#' @param conn 连接
#' @param FKeyWord 关键词
#'
#' @return 返回值
#' @export
#'
#' @examples
#' oildCard_selectDB()
oildCard_selectDB <- function(conn=tsda::conn_rds('nsic'),FKeyWord='ljiang1469') {
  sql <- paste0("SELECT FOrderSouce  订单来源渠道
      ,FTBId  淘宝ID
      ,FOrderId 订单号
      ,FLiYu 礼遇

      ,FDealerName 经销商名称
      ,FOrderPhone  拍单手机号
      ,FCar 车型

      ,FTmallOrderTime  天猫下单时间
      ,FLMSStatus  LMS下发状态
      ,FLMSOrderTime  LMS下单时间
      ,FLMSNewStatus  LMS最新状态
      ,FChannelSource 渠道来源
      ,FVIN 车架号
      ,FVerificationStatus 核销情况
      ,FTmallOrderTimeBeforeLMS  天猫下单时间早于LMS下单时间
      ,FJudgeFavorableComments 是否好评
      ,FJudgeRules_Cause 是否符合礼遇领取规则
      ,FExtendGiftTime 礼包预计发放时间
      ,FExtendGiftDelivery 礼包发放单号
      ,Faddress 收货地址

  FROM  t_ic_oilCard
  where FTBId = '",FKeyWord,"' or FOrderId ='",FKeyWord,"' or FOrderPhone ='",FKeyWord,"'")
  #print()
  res <- tsda::sql_select(conn,sql)
  return(res)

}
#' 查询油卡数据
#'
#' @param conn  连接
#'
#' @return 返回值
#' @export
#'
#' @examples
#' oildCard_selectDB_all()
oildCard_selectDB_all <- function(conn=tsda::conn_rds('nsic')) {
  sql <- paste0("SELECT FOrderSouce  订单来源渠道
      ,FTBId  淘宝ID
      ,FOrderId 订单号
      ,FLiYu 礼遇

      ,FDealerName 经销商名称
      ,FOrderPhone  拍单手机号
      ,FCar 车型

      ,FTmallOrderTime  天猫下单时间
      ,FLMSStatus  LMS下发状态
      ,FLMSOrderTime  LMS下单时间
      ,FLMSNewStatus  LMS最新状态
      ,FChannelSource 渠道来源
      ,FVIN 车架号
      ,FVerificationStatus 核销情况
      ,FTmallOrderTimeBeforeLMS  天猫下单时间早于LMS下单时间
      ,FJudgeFavorableComments 是否好评
      ,FJudgeRules_Cause 是否符合礼遇领取规则
      ,FExtendGiftTime 礼包预计发放时间
      ,FExtendGiftDelivery 礼包发放单号
      ,Faddress 收货地址

  FROM  t_ic_oilCard")
  #print()
  res <- tsda::sql_select(conn,sql)
  return(res)

}

#' 增值油卡订单查询功能
#'
#' @param conn 连接
#' @param FKeyWord 关键词
#'
#' @return 返回值
#' @export
#'
#' @examples
#' oildCard_selectDB2()
oildCard_selectDB2 <- function(conn=tsda::conn_rds('nsic'),FKeyWord='ljiang1469') {
  sql <- paste0("SELECT
      FTBId
      ,FOrderId
      ,FLiYu
	       ,FOrderPhone
		      ,FCar
			  ,FTmallOrderTime
			  ,FLMSStatus

			   ,FLMSOrderTime
      ,FLMSNewStatus

      ,FVIN
      ,FVerificationStatus
      ,FTmallOrderTimeBeforeLMS
      ,FJudgeFavorableComments
      ,FJudgeRules_Cause
      ,FExtendGiftTime
	  ,FExtendGiftDelivery

      ,Faddress
	  ,FRemarks

  FROM  t_ic_oilCard
  where FTBId = '",FKeyWord,"' or FOrderId ='",FKeyWord,"' or FOrderPhone ='",FKeyWord,"'")
  #print()
  data <- tsda::sql_select(conn,sql)
  ncount <- nrow(data)
  if(ncount >0){
    if(is.na(data$FExtendGiftDelivery)){
      #未单号
      msg <- paste0("TMALL ID: ",tsdo::na_replace(data$FTBId,""),"\n",
                    "订单号: ",tsdo::na_replace(data$FOrderId,""),"\n",
                    "礼遇： ",tsdo::na_replace(data$FLiYu,""),"\n",
                    "拍单手机号： ",tsdo::na_replace(data$FOrderPhone,""),"\n",
                    "车型： ",tsdo::na_replace(data$FCar,""),"\n",
                    "天猫下单时间： ",tsdo::na_replace(data$FTmallOrderTime,""),"\n",
                    "LMS下发状态: ",tsdo::na_replace(data$FLMSStatus,""),"\n",
                    "LMS下单时间： ",tsdo::na_replace(data$FLMSOrderTime,""),"\n",
                    "LMS最新状态: ",tsdo::na_replace(data$FLMSNewStatus,""),"\n",
                    "车架号： ",tsdo::na_replace(data$FVIN,""),"\n",
                    "核销情况： ",tsdo::na_replace(data$FVerificationStatus,""),"\n",
                    "天猫下单时间早于LMS下单时间: ",tsdo::na_replace(data$FTmallOrderTimeBeforeLMS,""),"\n",
                    "是否好评： ",tsdo::na_replace(data$FJudgeFavorableComments,""),"\n",
                    "是否符合礼遇领取规则或原因：",tsdo::na_replace(data$FJudgeRules_Cause,""),"\n",
                    "礼包预计发放时间： ",tsdo::na_replace(data$FExtendGiftTime,""),"\n",
                    "地址： ",tsdo::na_replace(data$Faddress,""),"\n",
                    "备注： ",tsdo::na_replace(data$FRemarks,""),"\n",
                    "很抱歉，这边已经将您的问题反馈至专员，目前还未收到专员回复，专员回复后我们这边会第一时间截图并留言给到您，给您带来不便，请见谅。"

          )
    }else{
      msg <- paste0("亲，经查询，您的订单号：",data$FOrderId,"，目前礼包已发放，物流单号为：",data$FExtendGiftDelivery)

    }


  }else{
    msg <- paste0("亲，您的订单我们正在核实,请稍等")

  }


  return(msg)

}



#' 处理油卡发货日期异常数据
#'
#' @param x 针对日期进行处理
#'
#' @return 返回值
#'
#' @examples
#' oilCard_formatDeliverDate()
oilCard_formatDeliverDate <- function(x) {

  if(is.na(x)){
    #针对空值进行处理
    res <-""
  }else{
    #处理其他情况
    nlen <- tsdo::len(x)
    if(nlen >0){
      #判断数据长度
      value <- try(as.numeric(x))
      if(is.na(value)){
        res <- x

      }else{
        #能够成功转换,变成日期后再转文本
        res <- as.character(as.Date(value,origin='1899-12-30'))
      }

    }else{
      res <-""
    }





  }


  return(res)
}



#' 针对日期数据进行批量处理
#'
#' @param data 数据
#'
#' @return 返回值
#' @export
#'
#' @examples
#' oilCard_formatDeliverDates()
oilCard_formatDeliverDates <- function(data){
  r <- lapply(data, oilCard_formatDeliverDate)
  res <- unlist(r)
  return(res)



}

#' 读取油卡数据
#'
#' @param file 文件名
#'
#' @return 返回值
#' @export
#'
#' @examples
#' oilCard_readExcel()
oilCard_readExcel <- function(file="data-raw/oilCardData.xlsx") {
  #library(readxl)
  oilCardData <- readxl::read_excel(file,    col_types = c("text", "text", "text",
                                                           "text", "text", "text", "text", "text",
                                                           "numeric", "text", "text", "date",
                                                           "text", "text", "text", "text", "text",
                                                           "text", "text", "text", "text", "text",
                                                           "text", "text", "text", "text"))
  #选择相应的数据
  data <-oilCardData[ ,1:25]
  #针对列进行重命名
  col_names <- c('FOrderSouce',
                  'FTBId',
                  'FOrderId',
                  'FLiYu',
                  'FDealerID',
                  'FDealerProvince',
                  'FDealerCity',
                  'FDealerName',
                  'FOrderPhone',
                  'FCar',
                  'FImportLocal',
                  'FTmallOrderTime',
                  'FLMSStatus',
                  'FLMSOrderTime',
                  'FLMSNewStatus',
                  'FChannelSource',
                  'FVIN',
                  'FVerificationStatus',
                  'FTmallOrderTimeBeforeLMS',
                  'FJudgeFavorableComments',
                  'FJudgeRules_Cause',
                  'FExtendGiftTime',
                  'FExtendGiftDelivery',
                  'Faddress',
                  'FRemarks')
 names(data) <- col_names

 data$FExtendGiftTime <- oilCard_formatDeliverDates(data$FExtendGiftTime)
 data$FOrderPhone <- as.character(data$FOrderPhone)
 data$FTmallOrderTime <- as.character(data$FTmallOrderTime)

  return(data)


}





#' 删除库中数据
#'
#' @param conn 连接
#'
#' @return 返回值
#' @export
#'
#' @examples
#' oilCard_backup_del()
oilCard_backup_del <- function(conn=tsda::conn_rds('nsic')) {
  sql_bak <- paste0("INSERT INTO [dbo].[t_ic_oilCardDel]
           ([FOrderSouce]
           ,[FTBId]
           ,[FOrderId]
           ,[FLiYu]
           ,[FDealerID]
           ,[FDealerProvince]
           ,[FDealerCity]
           ,[FDealerName]
           ,[FOrderPhone]
           ,[FCar]
           ,[FImportLocal]
           ,[FTmallOrderTime]
           ,[FLMSStatus]
           ,[FLMSOrderTime]
           ,[FLMSNewStatus]
           ,[FChannelSource]
           ,[FVIN]
           ,[FVerificationStatus]
           ,[FTmallOrderTimeBeforeLMS]
           ,[FJudgeFavorableComments]
           ,[FJudgeRules_Cause]
           ,[FExtendGiftTime]
           ,[FExtendGiftDelivery]
           ,[Faddress]
           ,[FRemarks]
           )
select * from t_ic_oilCard ")
  tsda::sql_update(conn,sql_bak)
  #删除数据
  sql_del <- paste0("delete from t_ic_oilCard")
  tsda::sql_update(conn,sql_del)

}


#' 油卡数据写入数据库
#'
#' @param file 文件
#' @param conn  连接
#'
#' @return 返回值
#' @export
#'
#' @examples
#' oilCard_writeDB()
oilCard_writeDB <- function(file="data-raw/oilCardData.xlsx",conn=tsda::conn_rds('nsic')){
  #删除库存数据
  oilCard_backup_del(conn=conn)

  #查询数据
  data <- oilCard_readExcel(file=file)
  #写入数据库
  tsda::db_writeTable(conn=conn,table_name = 't_ic_oilCard',r_object = data,append = TRUE)

}
