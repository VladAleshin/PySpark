## Pet project using pyspark module
<b>PySparkJob.py</b>
clickstream.parquet - parquet file with following structure:
<ul>
  <li>date  - день, в который происходят события </li>
<li>time - точное время события </li>
<li>event 	тип события, может быть или пока или клик по рекламе </li>
<li>platform 	платформа, на которой произошло рекламное событие </li>
<li>ad_id 	id рекламного объявления </li>
<li>client_union_id 	id рекламного клиента </li>
<li>campaign_union_id 	id рекламной кампании </li>
<li>ad_cost_type 	тип объявления с оплатой за клики (CPC) или за показы (CPM) </li>
<li>ad_cost 	стоимость объявления в рублях, для CPC объявлений - это цена за клик, для CPM - цена за 1000 показов </li>
<li>has_video 	есть ли у рекламного объявления видео </li>
<li>target_audience_count 	размер аудитории, на которую таргетируется объявление</li>
</ul>
