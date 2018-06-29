日志统计分析
-----------
1. 统计日志中各个状态码`（200, 404，500）`出现总的频次，并且按照小时时间窗，输出各个时间段各状态码的统计情况
2. 统计每个 IP 访问总的频次，并且按照小时时间窗，输出各个时间段各个IP 访问的情况。每个 IP 的统计信息是一个文件，并且以 IP 为文件名，如：`172.22.49.26.txt`
3. 统计每个接口(请求的 URL)访问总的频次，并且以接口为文件，按照秒为单位的时间窗，输出各个时间段各接口的访问情况。每个接口的统计信息是一个文件，如接口`/tour/category/query` 的统计文件命名为：`our-category-query.txt`
4. 统计每个接口的平均响应时间，并且以接口为分组，按照小时时间窗，输出各个时间段各个接口平均的响应时间。每个接口的统计信息是一个文件，如接口`/tour/category/query`的统计文件命名为：`tour-category-query.txt`
5. 接口访问频次预测，给`2015-09-08.log`到`2015-09-21.log` 共14天的日志文件，作为训练数据，设计预测算法来预测下一天（2015-09-22）每个小时窗内每个接口（请求的 URL）的访问总频次，评价指标按照`RMSE`指标