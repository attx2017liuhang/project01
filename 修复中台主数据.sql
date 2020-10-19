# 测试环境 10.251.76.11  yh_srm_channelcenter_test
select * from mid_product_status_clear_record;

CREATE TABLE `mid_product_status_clear_record` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `shop_group_code` varchar(20) NOT NULL DEFAULT '' COMMENT '店群编码',
  `shop_group_name` varchar(64) NOT NULL DEFAULT '' COMMENT '店群名称',
  `product_code` varchar(18) NOT NULL DEFAULT '' COMMENT '商品编码',
  `product_name` varchar(64) NOT NULL DEFAULT '' COMMENT '商品名称',
  `product_status` int(2) NOT NULL DEFAULT '-1' COMMENT '状态：0,0-B 正常商品;  2,2-A 新品;  3,3-H 停售;  6,6-L 退场;  7,7-K 永久停购;  9,9-E 暂时停购',
  `clear_type` tinyint(1) NOT NULL DEFAULT '0' COMMENT '清理类型：1、门店E状态自动转K状态；2、门店店群有效不可订转K；3、门店联营商品B状态转K',
  `is_delete` tinyint(4) NOT NULL DEFAULT '0' COMMENT '是否删除 0:正常；1:删除',
  `created_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `created_by` varchar(32) NOT NULL DEFAULT 'sys' COMMENT '创建人',
  `updated_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  `updated_by` varchar(32) NOT NULL DEFAULT 'sys' COMMENT '更新人',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1000000000 DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC COMMENT='无效商品清理记录中间表'



--2. 店群ABE, 且商品有效标识为2(有效不可订) (大数据支持)... -> 发起状态修改流程

create table b2b.mid_product_status_clear_record_k
as
select c.shop_group_code, d.shop_group_name, c.product_code, d.product_name, d.product_status, 2 as clear_type, small_category_code as category_code from
--2.过滤有效标识为2的商品
(
--1.找到店群下的单品只有一个商品标识的店群-单品组合
	select shop_group_code, product_code from
	(
		select location_code, shop_group_code from ods_supply_prd.base_location_info 
		where sdt = '20200705'
		and shop_status = 0
		and shop_group_code != ''
	) a
	join
	(
		select shop_code, product_code, valid_tag from ods_supply_prd.base_product_shop
		where sdt = '20200705'
		and product_status in (0, 2, 9)
	) b
	on a.location_code = b.shop_code
	group by shop_group_code, product_code
	having count(distinct valid_tag) = 1
)c
join
(
	select shop_group_code, shop_group_name, product_code, product_name, valid_tag, product_status, small_category_code from
	(
		select location_code, shop_group_code, shop_group_name from ods_supply_prd.base_location_info 
		where sdt = '20200705'
		and shop_status = 0
		and shop_group_code != ''
	) a
	join
	(
		select shop_code, product_code, product_name, valid_tag, product_status, small_category_code from ods_supply_prd.base_product_shop
		where sdt = '20200705'
		and product_status in (0, 2, 9)
	) b
	on a.location_code = b.shop_code
	group by shop_group_code, shop_group_name, product_code, product_name, valid_tag, product_status, small_category_code
) d
on c.shop_group_code = d.shop_group_code and c.product_code = d.product_code
where d.valid_tag = 2

--生产
sqoop export \
--connect jdbc:mysql://10.251.84.2:3306/yh_srm_channelcenter_prod \
--username bigdatarw \
--password 'HT6hstxgt64##' \
--table mid_product_status_clear_record \
--hcatalog-database b2b \
--hcatalog-table mid_product_status_clear_record_k \
--columns "shop_group_code, shop_group_name, product_code, product_name, product_status, clear_type, category_code" \
-m 10

--测试
sqoop export \
--connect jdbc:mysql://10.251.76.11:3306/yh_srm_channelcenter_test \
--username pc-mid-p_test \
--password 'pc-mid-p_test@2019' \
--table mid_product_status_clear_record \
--hcatalog-database b2b \
--hcatalog-table mid_product_status_clear_record_k \
--columns "shop_group_code, shop_group_name, product_code, product_name, product_status, clear_type, category_code" \
-m 10


--3. 联营B状态商品, 无销售>12个月 且 建档 > 12 个月(大数据支持) -> 发起状态修改流程
create table b2b.mid_product_status_clear_record_b_k
as
select c.shop_group_code, shop_group_name, product_code, product_name, 0 as product_status, 3 as clear_type, small_category_code as category_code from 
(
	select shop_group_code, shop_group_name, product_code, product_name, small_category_code from
	(
		select shop_group_code, shop_group_name, product_code, product_name, small_category_code, max(product_bracket_to_shop_create_time) as product_bracket_to_shop_create_time 
		from 
		(
			select location_code, shop_group_code, shop_group_name from ods_supply_prd.base_location_info 
			where sdt = '20200714'
			and shop_status = 0
			and shop_group_code != ''
		) a
		join
		(
			select shop_code, product_code, product_name, small_category_code, product_bracket_to_shop_create_time 
			from ods_supply_prd.base_product_shop
			where sdt = '20200714'
			and biz_type = 1
			and product_status = 0
		) b
		on a.location_code = b.shop_code
		group by shop_group_code, shop_group_name, product_code, product_name, small_category_code
	) x
	where datediff(substr(cast(now() as string),1,10), substr(from_unixtime(cast(cast(x.product_bracket_to_shop_create_time as bigint)/1000 as bigint),'yyyy-MM-dd HH:mm:ss'), 1, 10)) >= 365   
) c
left join
(
	select shop_group_code, goodsid from 
	(
		select location_code, shop_group_code from ods_supply_prd.base_location_info 
		where sdt = '20200714'
		and shop_status = 0
		and shop_group_code != ''
	) a
	join
	(
		select shop_id, goodsid from dw.sale_sap_dtl_fct 
		where sdt >= '20190714'
		and div_id >='10' and div_id <='14'
		and bill_type IN ('','S1','S2','ZF1','ZF2','ZR1','ZR2','ZFP','ZFP1')
	) b
	on a.location_code = b.shop_id
	group by shop_group_code, goodsid
) d
on c.shop_group_code = d.shop_group_code
and c.product_code = d.goodsid
where d.goodsid is NULL

--生产
sqoop export \
--connect jdbc:mysql://10.251.84.2:3306/yh_srm_channelcenter_prod \
--username bigdatarw \
--password 'HT6hstxgt64##' \
--table mid_product_status_clear_record \
--hcatalog-database b2b \
--hcatalog-table mid_product_status_clear_record_b_k \
--columns "shop_group_code, shop_group_name, product_code, product_name, product_status, clear_type, category_code" \
-m 10

--测试
sqoop export \
--connect jdbc:mysql://10.251.76.11:3306/yh_srm_channelcenter_test \
--username pc-mid-p_test \
--password 'pc-mid-p_test@2019' \
--table mid_product_status_clear_record \
--hcatalog-database b2b \
--hcatalog-table mid_product_status_clear_record_b_k \
--columns "shop_group_code, shop_group_name, product_code, product_name, product_status, clear_type, category_code" \
-m 10


--4. 非物流, 提取K状态商品, 业务异动日期（进销存）均超过12个月(大数据支持)
drop table b2b.mid_product_status_clear_record_n_d;
create table b2b.mid_product_status_clear_record_n_d
as 
select 
	base_info.shop_group_code, 
	base_info.shop_group_name, 
	base_info.product_code, 
	base_info.product_name, 
	base_info.product_status as product_status, 
	4 as clear_type, 
	base_info.small_category_code as category_code
from
(	
	select shop_group_code, shop_group_name, product_code, product_name, small_category_code, product_status 
	from 
	(
		select location_code, shop_group_code, shop_group_name 
		from ods_supply_prd.base_location_info 
		where sdt = '20200726' 
		and plant_type = 'A'
		and shop_status != 1
		and shop_group_code != ''
	) a
	join
	(
		select shop_code, product_code, product_name, small_category_code,	product_status
		from ods_supply_prd.base_product_shop
		where sdt = '20200726'
		and product_status = 7
		and product_code not regexp '[a-zA-Z].*'
	) b
	on a.location_code = b.shop_code 
	group by shop_group_code, shop_group_name, product_code, product_name, small_category_code, product_status
) base_info 
left join
--进价店群中的商品进货
(
	select shop_group_code, goodsid 
	from 
	(
		select location_code, shop_group_code, shop_group_name 
		from ods_supply_prd.base_location_info 
		where sdt = '20200726' 
		and plant_type = 'A'
		and shop_status != 1
		and shop_group_code != '' 
	) shop_group 
	inner join 
	(
		select shop_id, goodsid
		from
		(
			---针对物流是发货
			select receive_werksid as shop_id, goodsid 
			from b2b.receive_order_item 
			where sdt >= '20190726' 
			and order_type = 'A' 

			union all
			--门店退货
			select receive_werksid as shop_id, goodsid 
			from b2b.receive_order_item 
			where sdt >= '20190726' 
			and order_type = 'R' 
		) t 
		group by shop_id,goodsid
	) delivery 
	on shop_group.location_code = delivery.shop_id
	group by shop_group_code, goodsid 
) group_delivery
on base_info.shop_group_code = group_delivery.shop_group_code 
and base_info.product_code = group_delivery.goodsid
--进价店群中的商品销售
left join 
(
	select shop_group_code, goodsid 
	from 
	(
		select location_code, shop_group_code 
		from ods_supply_prd.base_location_info 
		where sdt = '20200723'
		and shop_status != 1
		and shop_group_code != ''
	) a
	join
	(
		select shop_id, goodsid from dw.sale_sap_dtl_fct 
		where sdt >= '20190723'
		and div_id >='10' 
		and div_id <='14'
		and bill_type IN ('','S1','S2','ZF1','ZF2','ZR1','ZR2','ZFP','ZFP1')
	) b
	on a.location_code = b.shop_id
	group by shop_group_code, goodsid
) group_sales
on base_info.shop_group_code = group_sales.shop_group_code 
and base_info.product_code = group_sales.goodsid
--进价店群中的商品库存
left join 
(
	select shop_group_code, goodsid 
	from 
	(
		select location_code, shop_group_code, shop_group_name 
		from ods_supply_prd.base_location_info 
		where sdt = '20200723' 
		and plant_type = 'B'
		and shop_status != 1
		and shop_group_code != '' 
	) shop_group 
	inner join 
	(
		select shop_id,goodsid 
		from dw.inv_setl_dly_fct 
		where sdt >= '20190723'
	) inv 
	on inv.shop_id=shop_group.location_code
	group by shop_group_code, goodsid
) group_inv 
on base_info.shop_group_code = group_inv.shop_group_code 
and base_info.product_code = group_inv.goodsid
where group_delivery.goodsid is NULL 
and group_sales.goodsid is NULL
and group_inv.goodsid is NULL

--生产
sqoop export \
--connect jdbc:mysql://10.251.84.2:3306/yh_srm_channelcenter_prod \
--username bigdatarw \
--password 'HT6hstxgt64##' \
--table mid_product_status_clear_record \
--hcatalog-database b2b \
--hcatalog-table mid_product_status_clear_record_n_d \
--columns "shop_group_code, shop_group_name, product_code, product_name, product_status, clear_type, category_code" \
-m 10

--测试
sqoop export \
--connect jdbc:mysql://10.251.76.11:3306/yh_srm_channelcenter_test \
--username pc-mid-p_test \
--password 'pc-mid-p_test@2019' \
--table mid_product_status_clear_record \
--hcatalog-database b2b \
--hcatalog-table mid_product_status_clear_record_n_d \
--columns "shop_group_code, shop_group_name, product_code, product_name, product_status, clear_type, category_code" \
-m 10


--5. 物流仓ABE配送商品60天内无业务异动（进出）、建档超过60天且当前无库存(大数据支持)
drop table b2b.mid_product_status_clear_record_y_d;
create table b2b.mid_product_status_clear_record_y_d
as 
select 
	base_info.shop_group_code, 
	base_info.shop_group_name, 
	base_info.product_code, 
	base_info.product_name, 
	base_info.product_status as product_status, 
	5 as clear_type, 
	base_info.small_category_code as category_code
from
(
	select shop_group_code, shop_group_name, product_code, product_name, small_category_code, product_status
	from
	(
		select shop_group_code, shop_group_name, product_code, product_name, small_category_code, product_status, max(product_bracket_to_shop_create_time) as product_bracket_to_shop_create_time 
		from 
		(
			select location_code, shop_group_code, shop_group_name 
			from ods_supply_prd.base_location_info 
			where sdt = '20200726' 
			and plant_type = 'B'
			and shop_status != 1
			and shop_group_code != '' 
		) a
		join
		(
			select shop_code, product_code, product_name, small_category_code, product_status, product_bracket_to_shop_create_time 
			from ods_supply_prd.base_product_shop
			where sdt = '20200726'
			and product_status in (0, 2, 9)
			and product_code not regexp '[a-zA-Z].*'
		) b
		on a.location_code = b.shop_code
		group by shop_group_code, shop_group_name, product_code, product_name, small_category_code,product_status
	) x
	where datediff(date_sub(current_date,0), substr(from_unixtime(cast(cast(x.product_bracket_to_shop_create_time as bigint)/1000 as bigint),'yyyy-MM-dd HH:mm:ss'), 1, 10)) >= 60   
) base_info 
---当前店群及商品没有进退货（包括针对物流的退货及反配）
left join 
(
	select shop_group_code, goodsid 
	from 
	(
		select location_code, shop_group_code, shop_group_name 
		from ods_supply_prd.base_location_info 
		where sdt = '20200726' 
		and plant_type = 'B'
		and shop_status != 1
		and shop_group_code != '' 
	) shop_group 
	inner join 
	(
		select shop_id,goodsid
		from
		(
			---针对物流是发货
			select substr(venderid,2,4) as shop_id,goodsid 
			from b2b.receive_order_item 
			where sdt >= '20200526' 
			and order_type = 'A' 
			and substr(venderid,1,2) = 'SW'

			union all
			---针对物流是进货
			select receive_werksid as shop_id,goodsid 
			from b2b.receive_order_item 
			where sdt >= '20200526' 
			and order_type = 'A' 
			and substr(receive_werksid,1,1) = 'W'

			union all
			select substr(venderid,2,4) as shop_id,goodsid 
			from b2b.receive_order_item 
			where sdt >= '20200526' 
			and order_type = 'R' 
			and substr(venderid,1,2) = 'SW'

			union all
			--针对物流到供应商的是退货
			select receive_werksid as shop_id,goodsid 
			from b2b.receive_order_item 
			where sdt >= '20200526' 
			and order_type = 'R' 
			and substr(receive_werksid,1,1) = 'W'

			union all
			---针对门店到物流的反配
			select substr(venderid,2,4) as shop_id,goodsid 
			from b2b.receive_order_item 
			where sdt >= '20200526' 
			and order_type = 'FP' 
			and substr(venderid,1,2) = 'SW'

			union all
			select receive_werksid as shop_id,goodsid 
			from b2b.receive_order_item 
			where sdt >= '20200526' 
			and order_type = 'FP' 
			and substr(receive_werksid,1,1) = 'W'
		) t 
		group by shop_id, goodsid
	) delivery 
	on shop_group.location_code = delivery.shop_id
) group_delivery 
on base_info.shop_group_code = group_delivery.shop_group_code 
and base_info.product_code = group_delivery.goodsid
----当前店群及商品没有库存
left join 
(
	select shop_group_code, goodsid 
	from 
	(
		select location_code, shop_group_code, shop_group_name 
		from ods_supply_prd.base_location_info 
		where sdt = '20200726' 
		and plant_type = 'B'
		and shop_status != 1
		and shop_group_code != '' 
	) shop_group 
	inner join 
	(
		select shop_id, goodsid 
		from dw.inv_setl_dly_fct 
		where sdt = '20200726'
	) inv 
	on inv.shop_id = shop_group.location_code
) group_inv 
on base_info.shop_group_code = group_inv.shop_group_code 
and base_info.product_code = group_inv.goodsid
where group_delivery.goodsid is NULL 
and group_inv.goodsid is NULL 

--生产
sqoop export \
--connect jdbc:mysql://10.251.84.2:3306/yh_srm_channelcenter_prod \
--username bigdatarw \
--password 'HT6hstxgt64##' \
--table mid_product_status_clear_record \
--hcatalog-database b2b \
--hcatalog-table mid_product_status_clear_record_y_d \
--columns "shop_group_code, shop_group_name, product_code, product_name, product_status, clear_type, category_code" \
-m 10

--测试
sqoop export \
--connect jdbc:mysql://10.251.76.11:3306/yh_srm_channelcenter_test \
--username pc-mid-p_test \
--password 'pc-mid-p_test@2019' \
--table mid_product_status_clear_record \
--hcatalog-database b2b \
--hcatalog-table mid_product_status_clear_record_y_d \
--columns "shop_group_code, shop_group_name, product_code, product_name, product_status, clear_type, category_code" \
-m 10





