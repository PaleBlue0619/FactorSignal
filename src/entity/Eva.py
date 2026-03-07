import pandas as pd
import dolphindb as ddb
from typing import Dict, List
from src.entity.Result import Result

class Eva(Result):
    def __init__(self, session: ddb.session):
        super().__init__(session)

    def initDef(self):
        # 初始化信号评价计算函数
        self.session.run("""
        // 函数定义
        def InsertData(DBName, TBName, data, batchsize){
            // 预防Out of Memory，分批插入数据，batchsize为每次数据的记录数
            start_idx = 0
            end_idx = batchsize
            krow = rows(data)
            do{
                slice_data = data[start_idx:min(end_idx,krow),]
                if (rows(slice_data)>0){
                loadTable(DBName, TBName).append!(slice_data);
                print(end_idx);
                }
                start_idx = start_idx + batchsize
                end_idx = end_idx + batchsize
            }while(start_idx < krow)
        };
        
        def signalStats(callBackDays, afterStatDays, signalDF){
            // 统计-1
            summaryStats = select symbol, tradeDate, factor, value, ret,
                callBackDays as `period,
                double(mcount(iif(value == 1, 1, NULL), callBackDays)) as posNum,
                double(mcount(iif(value == -1, 1, NULL), callBackDays)) as negNum,
                double(mcount(iif(value == 0, 1, NULL), callBackDays)) as zeroNum,
                condUp1D = iif(move(ret,-1)>0, 1, 0),
                condUp2D = iif((move(ret,-1)>0 and move(ret,-2)>0), 1, 0),
                condUp3D = iif((move(ret,-1)>0 and move(ret,-2)>0 and move(ret,-3)>0), 1, 0),
                condDown1D = iif(move(ret,-1)<0, 1, 0),
                condDown2D = iif((move(ret,-1)<0 and move(ret,-2)<0), 1, 0),
                condDown3D = iif((move(ret,-1)<0 and move(ret,-2)<0 and move(ret,-3)<0), 1, 0)
                from signalDF context by factor, symbol
                order by symbol, tradeDate
        
            // pos
            update summaryStats set consUp1DNumPos = double(nullFill(msum(iif(condUp1D == 1 and value == 1, 1, 0), callBackDays)-iif(condUp1D == 1 and value == 1, 1, 0),0)) context by factor, symbol
            update summaryStats set consUp1DRatePos = double(nullFill(consUp1DNumPos\posNum,0.0))
            update summaryStats set consUp2DNumPos = double(nullFill(msum(iif(condUp2D == 1 and value == 1, 1, 0), callBackDays)-msum(iif(condUp2D == 1 and value == 1, 1, 0), 2),0)) context by factor, symbol
            update summaryStats set consUp2DRatePos = double(nullFill(consUp2DNumPos\posNum,0.0))
            update summaryStats set consUp3DNumPos = double(nullFill(msum(iif(condUp3D == 1 and value == 1, 1, 0), callBackDays)-msum(iif(condUp3D == 1 and value == 1, 1, 0), 3),0)) context by factor, symbol
            update summaryStats set consUp3DRatePos = double(nullFill(consUp3DNumPos\posNum,0.0))
        
            update summaryStats set consDown1DNumPos = double(nullFill(msum(iif(condDown1D == 1 and value == 1, 1, 0), callBackDays)-iif(condDown1D == 1 and value == 1, 1, 0),0)) context by factor, symbol
            update summaryStats set consDown1DRatePos = double(nullFill(consDown1DNumPos\posNum,0.0))
            update summaryStats set consDown2DNumPos = double(nullFill(msum(iif(condDown2D == 1 and value == 1, 1, 0), callBackDays)-msum(iif(condDown2D == 1 and value == 1, 1, 0), 2),0)) context by factor, symbol
            update summaryStats set consDown2DRatePos = double(nullFill(consDown2DNumPos\posNum,0.0))
            update summaryStats set consDown3DNumPos = double(nullFill(msum(iif(condDown3D == 1 and value == 1, 1, 0), callBackDays)-msum(iif(condDown3D == 1 and value == 1, 1, 0), 3),0)) context by factor, symbol
            update summaryStats set consDown3DRatePos = double(nullFill(consDown3DNumPos\posNum,0.0))
        
            // neg
            update summaryStats set consUp1DNumNeg = double(nullFill(msum(iif(condUp1D == 1 and value == -1, 1, 0), callBackDays)-iif(condUp1D == 1 and value == -1, 1, 0),0)) context by factor, symbol
            update summaryStats set consUp1DRateNeg = double(nullFill(consUp1DNumNeg\negNum,0.0))
            update summaryStats set consUp2DNumNeg = double(nullFill(msum(iif(condUp2D == 1 and value == -1, 1, 0), callBackDays)-msum(iif(condUp2D == 1 and value == -1, 1, 0), 2),0)) context by factor, symbol
            update summaryStats set consUp2DRateNeg = double(nullFill(consUp2DNumNeg\negNum,0.0))
            update summaryStats set consUp3DNumNeg = double(nullFill(msum(iif(condUp3D == 1 and value == -1, 1, 0), callBackDays)-msum(iif(condUp3D == 1 and value == -1, 1, 0), 3),0)) context by factor, symbol
            update summaryStats set consUp3DRateNeg = double(nullFill(consUp3DNumNeg\negNum,0.0))
        
            update summaryStats set consDown1DNumNeg = double(nullFill(msum(iif(condDown1D == 1 and value == -1, 1, 0), callBackDays)-iif(condDown1D == 1 and value == -1, 1, 0),0)) context by factor, symbol
            update summaryStats set consDown1DRateNeg = double(nullFill(consDown1DNumNeg\negNum,0.0))
            update summaryStats set consDown2DNumNeg = double(nullFill(msum(iif(condDown2D == 1 and value == -1, 1, 0), callBackDays)-msum(iif(condDown2D == 1 and value == -1, 1, 0), 2),0)) context by factor, symbol
            update summaryStats set consDown2DRateNeg = double(nullFill(consDown2DNumNeg\negNum,0.0))
            update summaryStats set consDown3DNumNeg = double(nullFill(msum(iif(condDown3D == 1 and value == -1, 1, 0), callBackDays)-msum(iif(condDown3D == 1 and value == -1, 1, 0), 3),0)) context by factor, symbol
            update summaryStats set consDown3DRateNeg = double(nullFill(consDown3DNumNeg\negNum,0.0))
        
            // clear draftCols
            dropColumns!(summaryStats,`condUp1D`condDown1D`condUp2D`condUp3D`condDown2D`condDown3D)
        
            // unpivot
            keyColNames = ["symbol","tradeDate","factor","period"]
            valColNames = array(STRING,0)
            for (i in columnNames(summaryStats)){
                if (! (i in keyColNames) and !(i in ["ret", "value"])){
                    valColNames.append!(string(i))
                }
            }
            summaryStats0 = unpivot(summaryStats, keyColNames=keyColNames,valueColNames=valColNames)
        
            // 统计-2: 
            // K日涨跌幅
            for (day in afterStatDays){
                // retData prepare
                update summaryStats set mret = move(ret, callBackDays) context by factor,symbol
                update summaryStats set retKD = mprod(1+mret, day)-1 context by factor,symbol
                
                // retKD avg stats
                // pos
                update summaryStats set retAvgPos = double(nullFill((msum(iif(value == 1, 1, 0) * retKD, callBackDays)-msum(iif(value == 1, 1, 0) * retKD, day))\posNum,0)) context by factor, symbol 
                update summaryStats set upNumPos = double(nullFill(msum(iif(value == 1 and retKD > 0, 1, 0), callBackDays)-msum(iif(value == 1 and retKD > 0, 1, 0), day),0)) context by factor, symbol
                update summaryStats set upRatePos = double(nullFill(upNumPos\posNum,0)) context by factor, symbol
                update summaryStats set downNumPos = double(nullFill(msum(iif(value == 1 and retKD < 0, 1, 0), callBackDays)-msum(iif(value == 1 and retKD < 0, 1, 0), day),0)) context by factor, symbol
                update summaryStats set downRatePos = double(nullFill(downNumPos\posNum,0)) context by factor, symbol
                
                // neg
                update summaryStats set retAvgNeg = double(nullFill((msum(iif(value == -1, 1, 0) * retKD, callBackDays)-msum(iif(value == -1, 1, 0) * retKD, day))\negNum,0)) context by factor, symbol 
                update summaryStats set upNumNeg = double(nullFill(msum(iif(value == -1 and retKD > 0, 1, 0), callBackDays)-msum(iif(value == -1 and retKD > 0, 1, 0), day),0)) context by factor, symbol
                update summaryStats set upRateNeg = double(nullFill(upNumNeg\negNum,0)) context by factor, symbol
                update summaryStats set downNumNeg = double(nullFill(msum(iif(value == -1 and retKD < 0, 1, 0), callBackDays)-msum(iif(value == -1 and retKD < 0, 1, 0), day),0)) context by factor, symbol
                update summaryStats set downRateNeg = double(nullFill(downNumNeg\negNum,0)) context by factor, symbol
                
                // rename
                summaryStats[`retAvgPos+string(day)] = summaryStats["retAvgPos"]
                summaryStats[`upNumPos+string(day)] = summaryStats["upNumPos"]
                summaryStats[`upRatePos+string(day)] = summaryStats["upRatePos"]
                summaryStats[`downNumPos+string(day)] = summaryStats["downNumPos"]
                summaryStats[`downRatePos+string(day)] = summaryStats["downRatePos"]
                summaryStats[`retAvgNeg+string(day)] = summaryStats["retAvgNeg"]
                summaryStats[`upNumNeg+string(day)] = summaryStats["upNumNeg"]
                summaryStats[`upRateNeg+string(day)] = summaryStats["upRateNeg"]
                summaryStats[`downNumNeg+string(day)] = summaryStats["downNumNeg"]
                summaryStats[`downRateNeg+string(day)] = summaryStats["downRateNeg"]
                
                
                // unpivot
                keyColNames1 = ["symbol","tradeDate","factor","period"]
                valColNames1 = ["retAvgPos","upNumPos","upRatePos","downNumPos","downRatePos",
                                "retAvgNeg","upNumNeg","upRateNeg","downNumNeg","downRateNeg"
                                ]+string(day)
                summaryStats1 = unpivot(summaryStats, keyColNames=keyColNames1, valueColNames=valColNames1)
                summaryStats0.append!(summaryStats1);
            }
            undef(`summaryStats`summaryStats1)
            return summaryStats0
        }
        """)

    def eva(self, signalList: List[str]):
        self.session.upload({"factorList": signalList})
        """初始化定义"""
        startDate = pd.Timestamp(self.startDate).strftime("%Y.%m.%d")
        endDate = pd.Timestamp(self.endDate).strftime("%Y.%m.%d")
        self.session.run(rf"""
        // 参数配置
        startDate = {self.startDate}
        endDate = {self.endDate}
        callBackDays = 120
        afterStatDays = [3,4]
        barRetLabelName = "{self.barRetLabelName}"
        realStartDate = temporalAdd(startDate, -callBackDays, "d")
        realEndDate = endDate
        factorDB = "{self.factorDBName}"
        factorTB = "{self.factorTBName}"
        labelDB = "{self.labelDBName}"
        labelTB = "{self.labelTBName}"
        
        // 取数
        signalDF = select symbol,tradeDate,factor,value from loadTable(factorDB,factorTB) where factor in factorList and (tradeDate between realStartDate and realEndDate) 
        labelDF = select cont as symbol,tradeDate,value as ret from loadTable(labelDB,labelTB) where label == barRetLabelName and (tradeDate between realStartDate and realEndDate)
        signalDF = lj(signalDF, labelDF, `symbol`tradeDate)
        undef(`labelDF);
        
        // 进行统计        
        summaryStats = signalStats(callBackDays, afterStatDays, signalDF);
        
        // 插入至指定数据库
        InsertData(factorDB, factorTB, summaryStats, 1000000);
        undef(`signalDF`summaryStats);
        """)