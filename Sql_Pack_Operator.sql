POC_TEMPORAL_DATA_PACK_Plus_N_Jours_OPERATOR.sql

-- ================================================================================================================================
-- POC_TEMPORAL_DATA_PACK_Plus_N_Jours_OPERATOR.sql
--
--
-- COLLAPSE OPERATOR
-- C:\Sources\00_INFOCENTRE\_STUDIO\03_POC\POC_TEMPORAL_DATA_PACK_Plus_N_Jours_OPERATOR.sql
-- -------------------------------------------------------------------------------
-- ldnu 10/09/2018 15:16:33
-- -------------------------------------------------------------------------------

-- https://www.dcs.warwick.ac.uk/~hugh/CS253/CS253-Temporal-Data.pdf
-- https://www.elsevier.com/books/time-and-relational-theory/date/978-0-12-800631-3
-- https://technology.amis.nl/2014/06/08/sql-challenge-packing-time-intervals-and-merging-valid-time-periods/

-- -------------------------------------------------------------------------------
-- ldnu 26/09/2018 11:43:11
-- -------------------------------------------------------------------------------


-- JEU DE TEST
-- conn egs/egs@odsdev

drop table t1 purge;

create table t1 (id number not null, dtdeb date not null, dtfin date not null);
alter table t1 add constraint t1_pk primary key (id , dtdeb);
alter table t1 add constraint ckc01 check (dtdeb <= dtfin);

insert into t1 (id, dtdeb, dtfin) values (1, date '2017-01-01', date '9999-12-31');
insert into t1 (id, dtdeb, dtfin) values (2, date '2017-06-01', date '2018-03-31');
-- suit
insert into t1 (id, dtdeb, dtfin) values (3, date '2017-01-01', date '2017-06-30');
insert into t1 (id, dtdeb, dtfin) values (3, date '2017-07-01', date '2017-12-31');
-- trou
insert into t1 (id, dtdeb, dtfin) values (4, date '2017-01-01', date '2018-01-31');
insert into t1 (id, dtdeb, dtfin) values (4, date '2018-05-01', date '9999-12-31');
-- trou
insert into t1 (id, dtdeb, dtfin) values (5, date '2017-01-01', date '2017-04-30');
insert into t1 (id, dtdeb, dtfin) values (5, date '2017-09-01', date '2017-12-31');
insert into t1 (id, dtdeb, dtfin) values (5, date '2018-03-01', date '2018-05-31');
-- Suit + 30
insert into t1 (id, dtdeb, dtfin) values (5, date '2018-06-15', date '9999-12-31');

-- trou
insert into t1 (id, dtdeb, dtfin) values (6, date '2017-01-01', date '2017-04-30');
insert into t1 (id, dtdeb, dtfin) values (6, date '2017-09-01', date '2017-12-31');
insert into t1 (id, dtdeb, dtfin) values (6, date '2018-03-01', date '2018-05-31');
-- Suit + 30
insert into t1 (id, dtdeb, dtfin) values (6, date '2018-06-15', date '2018-09-01');
insert into t1 (id, dtdeb, dtfin) values (6, date '2018-10-01', date '2019-03-31');
insert into t1 (id, dtdeb, dtfin) values (6, date '2019-04-27', date '2019-07-31');
insert into t1 (id, dtdeb, dtfin) values (6, date '2019-10-01', date '2019-12-31');


select *
  from t1
order by id, dtdeb
;

commit;

<ok>
-- COLLAPSE 10g  -- PACK 10g avec 30 jours de tolerance pour la determination de la coincidente de pÈriode
with lData as (
select id, dtdeb, dtfin,
       (select min(a.dtdeb)
          from t1 a
         start with a.id    = t1.id
                and a.dtdeb = t1.dtdeb
          connect by prior id = id
                and prior dtdeb between dtdeb and (case when dtfin > date '9999-12-31' - 30 then dtfin else dtfin + 30 end)
                and prior dtdeb != dtdeb) as GRP
  from t1)
select id, GRP, min(dtdeb), max(dtfin)
 from lData
group by ID, GRP
order by ID, GRP;
</ok>

<ok>
-- COLLAPSE 11g -- PACK 11g
with
lData as (
select id, dtdeb, dtfin,
       lag(dtfin, 1, date '0000-01-01') over (partition by id order by dtdeb) as lag_dtfin,
       lag(dtdeb, 1, date '9999-12-31') over (partition by id order by dtdeb)  as lag_dtdeb,
       lag(id, 1, -1) over (partition by id order by dtdeb) as lag_id
from t1),
lDataFirst as (
select id, dtdeb, dtfin,
       case
       when ((id != lag_id) or (dtdeb not between lag_dtdeb and (case when lag_dtfin > date '9999-12-31' - 30 then lag_dtfin else lag_dtfin + 30 end) )) then
          1
          else
          0
        end x_first
  from lData),
lPeriod (pID,  pR_DTDEB, pDTDEB, pDTFIN) as (
select id, dtdeb, dtdeb, dtfin
  from lDataFirst
 where x_first = 1
union all
select t1.id, pR_DTDEB, t1.dtdeb, t1.dtfin
  from lPeriod a
        join t1
          on t1.ID    = a.pID
         and t1.DTDEB between a.pDTDEB and (case when a.pDTFIN > date '9999-12-31' - 30 then a.pDTFIN else a.pDTFIN + 30 end)
         and t1.DTDEB != a.pDTDEB)
select pID,  pR_DTDEB, max(pDTFIN) as DTFIN
  from lPeriod
group by pID, pR_DTDEB
order by pID, pR_DTDEB;
</ok>

create table x1 as
with lData as (
select IMMATRICULATION, POLICE, CODE_CIE, CODE_DECLA, PAYS, DTDEB, DTFIN,
       rank() over (order by IMMATRICULATION, POLICE, CODE_CIE, CODE_DECLA, PAYS) as grp
from rve.FVA_TMP_RVE2),
lData2 as (
select IMMATRICULATION, POLICE, CODE_CIE, CODE_DECLA, PAYS, DTDEB, DTFIN, GRP,
       lag(GRP, 1, -1)                  over (partition by IMMATRICULATION, POLICE, CODE_CIE, CODE_DECLA, PAYS order by DTDEB) as LAG_GRP,
       lag(DTFIN, 1, date '0000-01-01') over (partition by IMMATRICULATION, POLICE, CODE_CIE, CODE_DECLA, PAYS order by DTDEB) as LAG_DTFIN,
       lag(DTDEB, 1, date '9999-12-31') over (partition by IMMATRICULATION, POLICE, CODE_CIE, CODE_DECLA, PAYS order by DTDEB) as LAG_DTDEB
from lData),
lDataFirst as (
select IMMATRICULATION, POLICE, CODE_CIE, CODE_DECLA, PAYS, DTDEB, DTFIN,
       case
       when ((GRP != LAG_GRP) or (DTDEB not between LAG_DTDEB and (case when LAG_DTFIN > date '9999-12-31' - 30 then LAG_DTFIN else LAG_DTFIN + 30 end) )) then
          1
          else
          0
        end x_first
  from lData2),
lPeriod (pIMMATRICULATION, pPOLICE, pCODE_CIE, pCODE_DECLA, pPAYS,  pR_DTDEB, pDTDEB, pDTFIN) as (
select IMMATRICULATION, POLICE, CODE_CIE, CODE_DECLA, PAYS, DTDEB, DTDEB, DTFIN
  from lDataFirst
 where x_first = 1
union all
select t1.IMMATRICULATION, t1.POLICE, t1.CODE_CIE, t1.CODE_DECLA, t1.PAYS, pR_DTDEB, t1.DTDEB, t1.DTFIN
  from lPeriod a
        join rve.FVA_TMP_RVE2 t1
          on t1.IMMATRICULATION = a.pIMMATRICULATION
         and t1.POLICE          = a.pPOLICE
         and t1.CODE_CIE        = a.pCODE_CIE
         and t1.CODE_DECLA      = a.pCODE_DECLA
         and t1.PAYS            = a.pPAYS
         and t1.DTDEB between a.pDTDEB and (case when a.pDTFIN > date '9999-12-31' - 30 then a.pDTFIN else a.pDTFIN + 30 end)
         and t1.DTDEB != a.pDTDEB)
select pIMMATRICULATION, pPOLICE, pCODE_CIE, pCODE_DECLA, pPAYS, pR_DTDEB, max(pDTFIN) as DTFIN
  from lPeriod
group by pIMMATRICULATION, pPOLICE, pCODE_CIE, pCODE_DECLA, pPAYS, pR_DTDEB;

order by pIMMATRICULATION, pPOLICE, pCODE_CIE, pCODE_DECLA, pPAYS, pR_DTDEB;


-- -------------------------------------------------------------------------------
-- ldnu 10/09/2018 15:17:03
-- -------------------------------------------------------------------------------

-- JEU DE TEST
-- conn egs/egs@odsdev

drop table t1 purge;

create table t1 (id number not null, dtdeb date not null, dtfin date not null);
alter table t1 add constraint t1_pk primary key (id , dtdeb);
alter table t1 add constraint ckc01 check (dtdeb <= dtfin);

insert into t1 (id, dtdeb, dtfin) values (1, date '2017-01-01', date '9999-12-31');
insert into t1 (id, dtdeb, dtfin) values (2, date '2017-06-01', date '2018-03-31');
-- suit
insert into t1 (id, dtdeb, dtfin) values (3, date '2017-01-01', date '2017-06-30');
insert into t1 (id, dtdeb, dtfin) values (3, date '2017-07-01', date '2017-12-31');
-- trou
insert into t1 (id, dtdeb, dtfin) values (4, date '2017-01-01', date '2018-01-31');
insert into t1 (id, dtdeb, dtfin) values (4, date '2018-05-01', date '9999-12-31');
--
insert into t1 (id, dtdeb, dtfin) values (5, date '2017-01-01', date '2017-04-30');
insert into t1 (id, dtdeb, dtfin) values (5, date '2017-09-01', date '2017-12-31');
insert into t1 (id, dtdeb, dtfin) values (5, date '2018-03-01', date '9999-12-31');

--
-- overlap
insert into t1 (id, dtdeb, dtfin) values (6, date '2017-01-01', date '2017-06-30');
insert into t1 (id, dtdeb, dtfin) values (6, date '2017-05-01', date '2017-10-31');
-- overlap
insert into t1 (id, dtdeb, dtfin) values (6, date '2018-01-01', date '2018-03-31');
insert into t1 (id, dtdeb, dtfin) values (6, date '2018-02-01', date '2018-12-31');

insert into t1 (id, dtdeb, dtfin) values (7, date '2017-01-01', date '2017-04-30');
insert into t1 (id, dtdeb, dtfin) values (7, date '2017-04-30', date '2017-12-31');

insert into t1 (id, dtdeb, dtfin) values (7, date '2018-01-01', date '9999-12-31');


insert into t1 (id, dtdeb, dtfin) values (8, date '2017-01-01', date '2017-04-30');
insert into t1 (id, dtdeb, dtfin) values (8, date '2017-04-20', date '2017-05-31');
insert into t1 (id, dtdeb, dtfin) values (8, date '2017-05-25', date '2017-06-30');

-- overlap
insert into t1 (id, dtdeb, dtfin) values (8, date '2017-09-01', date '2017-12-31');
insert into t1 (id, dtdeb, dtfin) values (8, date '2018-11-01', date '9999-12-31');

commit;


select *
  from t1
 order by id, dtdeb;

-- FUSION PERIOD CONTIGUES
with lData as (
select id, dtdeb, dtfin,
       (select max(a.dtdeb)
          from t1 a
         start with a.id = t1.id
           and a.dtdeb = t1.dtdeb
          connect by prior id = id
              and prior dtfin = dtdeb - 1) as GRP
  from t1)
select id, GRP, min(dtdeb), max(dtfin)
 from lData
group by ID, GRP
order by ID, GRP;

<ok>
-- COLLAPSE 10g  -- PACK 10g
with lData as (
select id, dtdeb, dtfin,
       (select min(a.dtdeb)
          from t1 a
         start with a.id    = t1.id
                and a.dtdeb = t1.dtdeb
          connect by prior id = id
                 and prior dtdeb between dtdeb and dtfin
                 and prior dtdeb != dtdeb) as GRP
  from t1)
select id, GRP, min(dtdeb), max(dtfin)
 from lData
group by ID, GRP
order by ID, GRP;
</ok>

-- -------------------------------------------------------------------------------
-- ldnu 11/09/2018 11:19:05
-- -------------------------------------------------------------------------------
-- Period Date meets fusion (concatenete)
with lData as (
select id, dtdeb, dtfin,
       case
       when not (    nvl(lag(dtfin, 1) over (partition by id order by dtdeb), date '9999-12-31') = dtdeb - 1
                 and nvl(lag(id, 1) over (partition by id order by dtdeb), -1)  = id) then
         1
       else
         0
       end as X_FIRST
  from t1),
lPeriod (pID,  pR_DTDEB, pDTDEB, pDTFIN) as (
select id, dtdeb, dtdeb, dtfin
  from lData
 where x_first = 1
union all
select t1.id, pR_DTDEB, t1.dtdeb, t1.dtfin
  from lPeriod a
        join t1
          on a.pID    = t1.ID
         and a.pDTFIN = t1.DTDEB -1 )
select pID,  pR_DTDEB, max(pDTFIN) as DTFIN
  from lPeriod
group by pID, pR_DTDEB
order by pID, pR_DTDEB
;

-- -------------------------------------------------------------------------------
-- COLLAPSE 11g
-- -------------------------------------------------------------------------------
-- Period Date meets fusion (concatenete)
with lData as (
select id, dtdeb, dtfin,
       case
       when not (    nvl(lag(dtfin, 1) over (partition by id order by dtdeb), date '9999-12-31') = dtdeb - 1
                 and nvl(lag(id, 1) over (partition by id order by dtdeb), -1)  = id) then
         1
       else
         0
       end as X_FIRST
  from t1),
lPeriod (pID,  pR_DTDEB, pDTDEB, pDTFIN) as (
select id, dtdeb, dtdeb, dtfin
  from lData
 where x_first = 1
union all
select t1.id, pR_DTDEB, t1.dtdeb, t1.dtfin
  from lPeriod a
        join t1
          on a.pID    = t1.ID
         and a.pDTFIN = t1.DTDEB -1 )
select pID,  pR_DTDEB, max(pDTFIN) as DTFIN
  from lPeriod
group by pID, pR_DTDEB
order by pID, pR_DTDEB
;



with
lData as (
select id, dtdeb, dtfin,
       lag(dtfin, 1, date '9999-12-31') over (partition by id order by dtdeb) as lag_dtfin,
       lag(dtdeb, 1, date '9999-12-31') over (partition by id order by dtdeb)  as lag_dtdeb,
       lead(dtfin, 1, date '9999-12-31') over (partition by id order by dtdeb) as lead_dtfin,
       lead(dtdeb, 1, date '9999-12-31') over (partition by id order by dtdeb) as lead_dtdeb,
       lag(id, 1, -1) over (partition by id order by dtdeb) as lag_id,
       lead(id, 1, -1) over (partition by id order by dtdeb) as lead_id
from t1)
select id, dtdeb, dtfin,
       case
       when (   (id != lag_id)
             or dtdeb not between lag_dtdeb and lag_dtfiin)
             and
             (lead_dtdeb between dtdeb and dtfin) then
          1
        else
          0
        end xfirst
  from lData
order by id, dtdeb
;
with
lData as (
select id, dtdeb, dtfin,
       lag(dtfin, 1, date '0000-01-01') over (partition by id order by dtdeb) as lag_dtfin,
       lag(dtdeb, 1, date '9999-12-31') over (partition by id order by dtdeb)  as lag_dtdeb,
       lead(dtfin, 1, date '0000-01-01') over (partition by id order by dtdeb) as lead_dtfin,
       lead(dtdeb, 1, date '9999-12-31') over (partition by id order by dtdeb) as lead_dtdeb,
       lag(id, 1, -1) over (partition by id order by dtdeb) as lag_id,
       lead(id, 1, -1) over (partition by id order by dtdeb) as lead_id
from t1)
select id, dtdeb, dtfin,
       lag_dtfin, lag_dtdeb, lead_dtfin, lead_dtdeb, lag_id, lead_id,
       case
       when ( (not ((id = lag_id) and (dtdeb between lag_dtdeb and lag_dtfin)) )
             and
             ( (id = lead_id)  and (lead_dtdeb between dtdeb and dtfin)) ) then
          1
          else
          0
        end xfirst
  from lData
order by id, dtdeb
;


with
lData as (
select id, dtdeb, dtfin,
       lag(dtfin, 1, date '0000-01-01') over (partition by id order by dtdeb) as lag_dtfin,
       lag(dtdeb, 1, date '9999-12-31') over (partition by id order by dtdeb)  as lag_dtdeb,
       lead(dtfin, 1, date '0000-01-01') over (partition by id order by dtdeb) as lead_dtfin,
       lead(dtdeb, 1, date '9999-12-31') over (partition by id order by dtdeb) as lead_dtdeb,
       lag(id, 1, -1) over (partition by id order by dtdeb) as lag_id,
       lead(id, 1, -1) over (partition by id order by dtdeb) as lead_id
from t1),
lDataFirst as (
select id, dtdeb, dtfin,
       lag_dtfin, lag_dtdeb, lead_dtfin, lead_dtdeb, lag_id, lead_id,
       case
       when ( (not ((id = lag_id) and (dtdeb between lag_dtdeb and lag_dtfin)) )
             and
             ( (id = lead_id)  and (lead_dtdeb between dtdeb and dtfin)) ) then
          1
          else
          0
        end x_first
  from lData)
lPeriod (pID,  pR_DTDEB, pDTDEB, pDTFIN) as (
select id, dtdeb, dtdeb, dtfin
  from lDataFirst
 where x_first = 1
union all
select t1.id, pR_DTDEB, t1.dtdeb, t1.dtfin
  from lPeriod a
        join t1
          on t1.ID    = a.pID
         and t1.DTDEB between a.pDTDEB and a.pDTFIN
         and t1.DTDEB != a.pDTDEB)
select pID,  pR_DTDEB, max(pDTFIN) as DTFIN
  from lPeriod
group by pID, pR_DTDEB
order by pID, pR_DTDEB
;




with
lData as (
select id, dtdeb, dtfin,
       lag(dtfin, 1, date '0000-01-01') over (partition by id order by dtdeb) as lag_dtfin,
       lag(dtdeb, 1, date '9999-12-31') over (partition by id order by dtdeb)  as lag_dtdeb,
       lead(dtfin, 1, date '0000-01-01') over (partition by id order by dtdeb) as lead_dtfin,
       lead(dtdeb, 1, date '9999-12-31') over (partition by id order by dtdeb) as lead_dtdeb,
       lag(id, 1, -1) over (partition by id order by dtdeb) as lag_id,
       lead(id, 1, -1) over (partition by id order by dtdeb) as lead_id
from t1)
select id, dtdeb, dtfin,
       lag_dtfin, lag_dtdeb, lead_dtfin, lead_dtdeb, lag_id, lead_id,
       case
       when ((id != lag_id) or (dtdeb not between lag_dtdeb and lag_dtfin)) then
          1
          else
          0
        end x_first,
       case
       when ( (not ((id = lag_id) and (dtdeb between lag_dtdeb and lag_dtfin)) )
             and
             ( (id = lead_id)  and (lead_dtdeb between dtdeb and dtfin)) ) then
          1
          else
          0
        end x_____first
  from lData
order by id, dtdeb
;





<ok>
-- COLLAPSE 11g -- PACK 11g
with
lData as (
select id, dtdeb, dtfin,
       lag(dtfin, 1, date '0000-01-01') over (partition by id order by dtdeb) as lag_dtfin,
       lag(dtdeb, 1, date '9999-12-31') over (partition by id order by dtdeb)  as lag_dtdeb,
       lag(id, 1, -1) over (partition by id order by dtdeb) as lag_id
from t1),
lDataFirst as (
select id, dtdeb, dtfin,
       case
       when ((id != lag_id) or (dtdeb not between lag_dtdeb and lag_dtfin)) then
          1
          else
          0
        end x_first
  from lData),
lPeriod (pID,  pR_DTDEB, pDTDEB, pDTFIN) as (
select id, dtdeb, dtdeb, dtfin
  from lDataFirst
 where x_first = 1
union all
select t1.id, pR_DTDEB, t1.dtdeb, t1.dtfin
  from lPeriod a
        join t1
          on t1.ID    = a.pID
         and t1.DTDEB between a.pDTDEB and a.pDTFIN
         and t1.DTDEB != a.pDTDEB)
select pID,  pR_DTDEB, max(pDTFIN) as DTFIN
  from lPeriod
group by pID, pR_DTDEB
order by pID, pR_DTDEB;
</ok>


-- -------------------------------------------------------------------------------
-- UNPACK

<ok>
-- EXPAND 10g -- UNPACK 10g
with
lBornes as (
select id, min(dtdeb) - 1 as dtdeb, max(dtfin) as dtfin
  from t1
where id = 6
group by id),
lData as (
select id, level as niveau,
       dtdeb + level as jour
  from lBornes
connect by dtdeb + level <= dtfin
)
select distinct a.id, a.jour
  from lData a
       join t1 b
         on b.id = a.id
        and a.jour between b.dtdeb and b.dtfin
order by a.id, a.jour;


-- EXPAND 11g -- UNPACK 11g
with
lBornes as (
select id, min(dtdeb) - 1 as dtdeb, max(dtfin) as dtfin
  from t1
where id = 6
group by id),
lIterData (pID, pJour, pDtfin) as (
select id, dtdeb, dtfin
  from lBornes
union all
select pID, pJour + 1, pDtfin
  from lIterData
 where pJour <= pDtfin)
select distinct a.pID, a.pJour
  from lIterData a
       join t1 b
         on b.id = a.pId
        and a.pJour between b.dtdeb and b.dtfin
order by a.pId, a.pJour;
</ok>















-- -------------------------------------------------------------------------------
-- -------------------------------------------------------------------------------
-- -------------------------------------------------------------------------------
-- -------------------------------------------------------------------------------


-- COLLAPSE
with lData as (
select id, dtdeb, dtfin,
       (select min(a.dtdeb)
          from t1 a
         start with a.id    = t1.id
                and a.dtdeb = t1.dtdeb
          connect by prior id = id
                 and prior dtdeb between dtdeb and dtfin
                 and prior dtdeb != dtdeb) as GRP
  from t1)
select id, GRP, min(dtdeb), max(dtfin)
 from lData
group by ID, GRP
order by ID, GRP;

-- OK
with
lPeriod (pID, pDTDEB, pDTFIN) as (
  select ID, DTDEB, DTFIN
    from t1
   where ID = 6
     and DTDEB = date '2017-05-01'
union all
  select pID, a.DTDEB, a.DTFIN
    from t1 a
         join lPeriod b
           on b.pID = a.ID
          and pDTDEB between a.DTDEB and a.DTFIN
          and pDTDEB != a.DTDEB)
select min(pDTDEB)
  from lPeriod
;



with lData as (
select id, dtdeb, dtfin,
       (with
       lPeriod (pID, pDTDEB, pDTFIN) as (
         select ID, DTDEB, DTFIN
           from t1 t
          where t.ID    = t1.ID
            and t.DTDEB = t1.DTDEB
       union all
         select pID, a.DTDEB, a.DTFIN
           from t1 a
                join lPeriod b
                  on b.pID = a.ID
                 and pDTDEB between a.DTDEB and a.DTFIN
                 and pDTDEB != a.DTDEB)
       select min(pDTDEB)
         from lPeriod
       ) as GRP
  from t1)
select id, GRP, min(dtdeb), max(dtfin)
 from lData
group by ID, GRP
order by ID, GRP;

ORA-32034: unsupported use of WITH clause
32034. 00000 -  "unsupported use of WITH clause"
*Cause:    Inproper use of WITH clause because one of the following two reasons
           1. nesting of WITH clause within WITH clause not supported yet
           2. For a set query, WITH clause can't be specified for a branch.
           3. WITH clause cannot be specified within parenthesis.
*Action:   correct query and retry
Error at Line: 3 Column: 9
--'

with lData as (
select id, dtdeb, dtfin,
       (with
       lPeriod (pID, pDTDEB, pDTFIN) as (
         select ID, DTDEB, DTFIN
           from t1 t
          where t.ID    = t1.ID
            and t.DTDEB = t1.DTDEB
       union all
         select pID, a.DTDEB, a.DTFIN
           from t1 a
                join lPeriod b
                  on b.pID = a.ID
                 and pDTDEB between a.DTDEB and a.DTFIN
                 and pDTDEB != a.DTDEB)
       select min(pDTDEB)
         from lPeriod
       ) as GRP
  from t1)



-- -------------------------------------------------------------------------------
-- ldnu 10/09/2018 17:18:40
-- -------------------------------------------------------------------------------
-- -------------------------------------------------------------------------------
-- ldnu 10/09/2018 17:18:41
-- -------------------------------------------------------------------------------
-- recursive CTE
with EACH_LEVEL (empno, name, mgr) as
( --
  -- start with
  --
  select empno, ename, mgr
  from   emp
  where  mgr is null
  --
  -- connect by
  --
  union all
  select emp.empno, emp.ename, emp.mgr
  from   emp, EACH_LEVEL
  where  emp.mgr = each_level.empno
)
select *
from   each_level;





"	"with each_level (empno, name, mgr, rlevel) as
( select empno, ename, mgr, 1 rlevel
  from   emp
  where  mgr is null
  union all
  select emp.empno, emp.ename, emp.mgr, rlevel+1
  from   emp, each_level
  where  emp.mgr = each_level.empno
)
select * from each_level;

-- Building the concatenated list of employee names
with each_level (empno, name) as
( select empno, ename from  emp
  where  mgr is null
  union all
  select e.empno,
         each_level.name||'-'||e.ename
  from   emp e, each_level
  where  e.mgr = each_level.empno
)
select empno, name from each_level;"	"-- Recursive WITH definition detecting cycle and returning error message
with each_level (empno, name, mgr) as
( select empno, ename, mgr
  from   emp
  where  ename = 'KING'
  union all
  select emp.empno, emp.ename, emp.mgr
  from   emp, each_level
  where  emp.mgr = each_level.empno
)
select *
from   each_level;
-- ORA-32044: cycle detected while executing recursive WITH query

-- Recursive WITH, cycle, CYCLE clause, and new column
with each_level (empno, name, mgr) as
( select empno, ename, mgr from emp
  where  ename = 'KING'
  union all
  select emp.empno, emp.ename, emp.mgr
  from   emp, each_level
  where  emp.mgr = each_level.empno )
CYCLE mgr SET is_cycle TO 'Y' DEFAULT 'N'
select * from each_level;"	"-- Listing 16: Recursive CTE, SEARCH, and SET clause
with each_level (empno, name, hiredate, mgr) as
( select empno, ename, hiredate, mgr from emp
  where  ename = 'KING'
  union all
  select e.empno,
    each_level.name||'-'||e.ename, e.hiredate, e.mgr
  from   emp e, each_level
  where  e.mgr = each_level.empno )
SEARCH BREADTH FIRST BY HIREDATE SET IDX
select name, hiredate, idx  from each_level;

with each_level (empno, name, hiredate, mgr) as
( select empno, ename, hiredate, mgr from emp
  where  ename = 'KING'
  union all
  select e.empno,
    each_level.name||'-'||e.ename, e.hiredate, e.mgr
  from   emp e, each_level
  where  e.mgr = each_level.empno )
SEARCH DEPTH FIRST BY HIREDATE SET IDX
select name, hiredate, idx  from each_level;"	"SQL> select * from messages;
TXT
--------------------------------------------------------------------------
I caught up with Connor and Maria Colgan today. They have taken over
AskTOM for Oracle Developers
sql> select * from twitter_handles;
  ID  TERM                        HANDLE
óóóó  óóóóóóóóóóóóóóóóóóóóóóóóóó  óóóóóóóóóóóóóóó
   1  Connor McDonald             @connor_mc_d
   2  Connor                      @connor_mc_d
   3  Maria Colgan                @sqlmaria
   4  Oracle Developers           @otndev
   5  Oracle                      @oracle
   6  AskTOM                      @oracleasktom


with
  tweetised(ind,tweet_txt)  as
(
  select 1 ind, txt tweet_txt
  from   messages
  union all
  select ind+1, replace(tweet_txt,term,handle)
  from   tweetised, twitter_handles
  where  ind = id
)
select * from tweetised;"	"-- Listing 19: Picking the last row with FETCH FIRST
with
tweetised(ind,tweet_txt)  as
(
  select 1 ind, txt tweet_txt
  from   messages
  union all
  select ind+1, replace(tweet_txt,term,handle)
  from   tweetised, twitter_handles
  where  ind = id
)
select * from tweetised
order by ind desc
fetch first 1 row only;

"	https://blogs.oracle.com/oraclemagazine/old-dog%2c-new-tricks%2c-part-2
--'
"create table test_hier (id number primary key, label varchar2(20), id_pere number);
insert into test_hier (id, label, id_pere) values (1, 'BOB', null);
insert into test_hier (id, label, id_pere) values (2, 'JEAN', 1);
insert into test_hier (id, label, id_pere) values (3, 'MARIE', 1);
insert into test_hier (id, label, id_pere) values (4, 'JACQUES', 2);
insert into test_hier (id, label, id_pere) values (5, 'PIERRE', 2);
commit;


column level format 999
column chemin format a30
column id format 999
column label format a20
column id_pere format 999"	"-- Descente de l'arbre  ------------------------------------------------------------------------------
select level, sys_connect_by_path(label, ' / ' ) as chemin, a.*
  from test_hier a
 start with label = 'BOB'
  connect by id_pere = prior id;

with lDescenteArbre(pLevel, pChemin, pLabel, pId, pId_Pere) as (
select 1, label, label, id, id_pere
  from test_hier
 where id = 1
union all
select pLevel + 1, pChemin || ' / ' || label, label, id, id_pere
  from test_hier a
       join lDescenteArbre b
        on a.id_Pere = b.pId)
select pLevel, pChemin, pLabel, pId, pId_Pere
  from lDescenteArbre
order by pLevel, pLabel;
-- RemontÈ de l'arbre -------------------------------------------------------------------------
select level, sys_connect_by_path(label, ' / ' ) as chemin, a.*
  from test_hier a
 start with label = 'PIERRE'
  connect by prior id_pere = id;

with lRemonteArbre(pLevel, pChemin, pLabel, pId, pId_Pere) as (
select 1, label, label, id, id_pere
  from test_hier
 where id = 5
union all
select pLevel + 1, pChemin || ' / ' || label, label, id, id_pere
  from test_hier a
       join lRemonteArbre b
         on b.pId_Pere = a.id)
select pLevel, pChemin, pLabel, pId, pId_Pere
  from lRemonteArbre
order by pLevel, pLabel;

-- Generator
with lGen(pLevel) as (
select 1
  from dual
union all
select pLevel + 1
  from lGen
 where pLevel < 10)
select pLevel
  from lGen;"	"select level,
     lpad('*', 2*level, '*')||ename nm
  from emp
    start with mgr is null
    connect by prior empno = mgr
    order siblings by ename;
 with emp_data(ename,empno,mgr,l)
    as
     (select ename, empno, mgr, 1 lvl from emp where mgr is null
      union all
      select emp.ename, emp.empno, emp.mgr, ed.l+1
        from emp, emp_data ed
       where emp.mgr = ed.empno
    )
    SEARCH DEPTH FIRST BY ename SET order_by
   select l,
         lpad('*' ,2*l, '*')||ename nm
     from emp_data
    order by order_by;"	" with data(r)
   as
      (select 1 r from dual
       union all
      select r+1 from data where r < 5
      )
   select r, sysdate+r
       from data;

"	"with lIter(pLabel, pLettre) as (
select 'ABC' as label, substr('ABC', length('ABC'), 1) as lettre
  from dual
union all
select substr(pLabel, 1, length(pLabel) - 1), substr(pLabel, length(pLabel) - 1, 1)
  from lIter
 where length(pLabel) > 1)
select pLabel, pLettre
  from lIter;"	"with lLabel as (
select 'ABC' as label
  from dual),
lIter(pLabel, pLettre) as (
select label, substr(label, length(label), 1) as lettre
  from lLabel
union all
select substr(pLabel, 1, length(pLabel) - 1), substr(pLabel, length(pLabel) - 1, 1)
  from lIter
 where length(pLabel) > 1)
select pLabel, pLettre
  from lIter;"	"with lLabel as (
select 'franceculture' as label
  from dual),
lIter(pLabel, pLettre) as (
select label, substr(label, length(label), 1) as lettre
  from lLabel
union all
select substr(pLabel, 1, length(pLabel) - 1), substr(pLabel, length(pLabel) - 1, 1)
  from lIter
 where length(pLabel) > 1)
select pLabel, pLettre,
       count(*) over (partition by pLettre) as cnt
  from lIter
order by 2;"	"with lFactoriel(pNombre, pAcc) as (
select 17 as nombre, 1 as acc
  from dual
union all
select pNombre - 1, pAcc * pNombre
  from lFactoriel
 where pNombre > 1)
select pNombre, pAcc
  from lFactoriel;"	"create or replace procedure pgcd(pN1 in pls_integer, pN2 in pls_integer) is
   lReste   pls_integer;
begin
   dbms_output.put_line('N1 = ' || pN1 || ' ; N2 = ' || pN2);
   lReste := mod(pN1, pN2);
   if lReste = 0 then
     dbms_output.put_line('RÈsultat = ' || pN2);
   else
     pgcd(pN2, lReste);
   end if;
end pgcd;
/
show errors
set serveroutput on
-- assert (N1 >= N2)  -- pas de test par la procÈdure
exec pgcd(10, 5);
exec pgcd(108, 3);
exec pgcd(109, 3);
exec pgcd(8479, 61);
exec pgcd(847912, 8)"	"with lPgcd(pN1, pN2, pResultat) as (
select 78 as N1, 4 as N2, 1 as Res
  from dual
union all
select pN2, mod(pN1, pN2), mod(pN1, pN2)
  from lPgcd
 where mod(pN1, pN2) > 0)
select pN1, pN2, pResultat
  from lPgcd;"	"with lSuite(pIndice, pN1) as (
select 1 as Indice, 7244656 as N1
  from dual
union all
select pIndice + 1,
       case
       when mod(pN1, 2) = 0 then
         pN1 / 2
       else
         pN1 * 3 + 1
       end as N1
  from lSuite
 where pN1 != 1)
select PINDICE, PN1
  from lSuite;"	"http://www.wolframalpha.com/input/?i=5x%5E3-3x%5E2%2B4x-9%2C+x%3D2  -- 5x^3-3x^2+4x-9, x=2
-- 5              = 5
-- (5 * 2) - 3    = 7
-- (7 * 2) + 4    = 18
-- (18 * 2) - 9   = 27
with lPolynome as (
select  5 as C,  3 as degre from dual union all
select -3 as C,  2 as degre from dual union all
select  4 as C,  1 as degre from dual union all
select -9 as C,  0 as degre from dual),
lX as (
select 2 as X from dual),
lHorner(pX, pDegre, pResultat) as (
select X, Degre - 1, C
  from lPolynome
       cross join lX
 where degre = (select max(degre) from lPolynome)
union all
select h.pX, pDegre - 1, (h.pX * h.pResultat) + p.C
  from lHorner h
       join lPolynome p
         on p.degre = h.pDegre
 where h.pDegre >= 0)
select pX, pDegre, pResultat
  from lHorner;"	https://asktom.oracle.com/pls/asktom/f?p=100:11:0::::P11_QUESTION_ID:9522561800346247672	"with
lAna(pIndice, pStr) as (
select length('abc') as Indice, 'abc'
  from dual
union all
select pIndice - 1,
       substr(pStr, 2, 32000)
  from lAna
 where length(pStr) > 1)
select *
  from lAna;"	"with
lAna(pIndice, pStr) as (
select length('abc') as Indice, 'abc'
  from dual
union all
select pIndice - 1,
       substr(pStr, 2, 32000)
  from lAna
 where pIndice > 1)
select *
  from lAna;
"	"with
lConst as (
select 'abc' as Str
 from dual),
lAna(pIndice, pLetter, pStr) as (
select length(Str) - 1 as Indice, substr(Str, 1, 1), substr(Str, 2, 32000)
  from lConst
union all
select pIndice - 1,
       substr(pStr, 1, 1),
       substr(pStr, 2, 32000)
  from lAna
 where length(pStr) > 0)
select pLetter, count(*)
  from lAna
group by pLetter
order by pLetter  ;"	"with
lConst as (
select replace('le droit a la securite', ' ', '') as Str
 from dual),
lAna(pIndice, pLetter, pStr) as (
select length(Str) - 1 as Indice, substr(Str, 1, 1), substr(Str, 2, 32000)
  from lConst
union all
select pIndice - 1,
       substr(pStr, 1, 1),
       substr(pStr, 2, 32000)
  from lAna
 where length(pStr) > 0)
select pLetter, count(*)
  from lAna
group by pLetter
order by 2 desc, pLetter  ;
-- http://www.lefigaro.fr/langue-francaise/actu-des-mots/2016/11/21/37002-20161121ARTFIG00063-l-anagramme-du-jour.php"	"with
lConst as (
select replace('la crise de l autorite', ' ', '') as Str
 from dual),
lAna(pIndice, pLetter, pStr) as (
select length(Str) - 1 as Indice, substr(Str, 1, 1), substr(Str, 2, 32000)
  from lConst
union all
select pIndice - 1,
       substr(pStr, 1, 1),
       substr(pStr, 2, 32000)
  from lAna
 where length(pStr) > 0)
select pLetter, count(*)
  from lAna
group by pLetter
order by 2 desc, pLetter  ;
-- http://www.lefigaro.fr/langue-francaise/actu-des-mots/2016/11/21/37002-20161121ARTFIG00063-l-anagramme-du-jour.php"	"-- pi day : https://connormcdonald.wordpress.com/2017/03/15/pi-day-march-14/
with
term(numerator, product, seq) as (
select sqrt(2) as numerator, sqrt(2) / 2 as product , 1 as seq
  from dual
union all
select sqrt(2 + numerator), sqrt(2 + numerator) * product / 2 , seq + 1
  from term
       join dual
         on term.seq <= 16)
select 2 / product as pi, numerator, seq
  from term
 where seq = 16;"	"with
lSecuBMP as
(select USR_MNEMONIQUE,
        APPL_VAL_VALUE                 as CLI_ID
   from gdv.V_BI_CLIENTS@LNA_DBLINK
  where APR_APPL_CODE  = 'BMP'
    and USR_MNEMONIQUE is not null),
lIter (INDICE, CLE, LABEL, TOKEN) as (
select 2                                      as INDICE,
       USR_MNEMONIQUE                         as CLE,
       CLI_ID                                 as LABEL,
       REGEXP_SUBSTR(CLI_ID,'[^;]+', 1, 1)     as TOKEN
 from lSecuBMP
union all
select b.INDICE + 1                                as INDICE,
       a.USR_MNEMONIQUE                            as CLE,
       a.CLI_ID                                    as LABEL,
       REGEXP_SUBSTR(b.LABEL,'[^;]+', 1, b.INDICE)  as TOKEN
 from lSecuBMP a
      join lIter b
        on b.CLE = a.USR_MNEMONIQUE
       and REGEXP_SUBSTR(b.LABEL,'[^;]+',1, b.INDICE) is not null)
CYCLE CLE SET IS_CYCLE TO 1 DEFAULT 0
select /*INDICE,*/ CLE, /*LABEL,*/ TOKEN
  from lIter
order by CLE, TOKEN;
"	"with
lSecuBMP as
(select 'BSNI' as USR_MNEMONIQUE, 'NTX'       as CLI_ID from dual union all
select 'ELLR' as USR_MNEMONIQUE, 'LBP'       as CLI_ID from dual union all
select 'JCEA' as USR_MNEMONIQUE, 'LBP'       as CLI_ID from dual union all
select 'JGQE' as USR_MNEMONIQUE, 'LBP'       as CLI_ID from dual union all
select 'JPTR' as USR_MNEMONIQUE, 'NTX'       as CLI_ID from dual union all
select 'MMIE' as USR_MNEMONIQUE, 'LBP'       as CLI_ID from dual union all
select 'ODPU' as USR_MNEMONIQUE, 'LBP;BRSMA' as CLI_ID from dual union all
select 'PCRT' as USR_MNEMONIQUE, 'LBP'       as CLI_ID from dual union all
select 'SRAN' as USR_MNEMONIQUE, 'LBP'       as CLI_ID from dual union all
select 'VPTN' as USR_MNEMONIQUE, 'BRSMA'     as CLI_ID from dual),
lIter (INDICE, CLE, LABEL, TOKEN) as (
select 2                                      as INDICE,
       USR_MNEMONIQUE                         as CLE,
       CLI_ID                                 as LABEL,
       REGEXP_SUBSTR(CLI_ID,'[^;]+',1, 1)     as TOKEN
 from lSecuBMP
union all
select b.INDICE + 1                                as INDICE,
       a.USR_MNEMONIQUE                            as CLE,
       a.CLI_ID                                    as LABEL,
       REGEXP_SUBSTR(b.LABEL,'[^;]+',1, b.INDICE)  as TOKEN
 from lSecuBMP a
      join lIter b
        on b.CLE = a.USR_MNEMONIQUE
       and REGEXP_SUBSTR(b.LABEL,'[^;]+',1, b.INDICE) is not null)
CYCLE CLE SET IS_CYCLE TO 1 DEFAULT 0
select INDICE, CLE, /*LABEL,*/ TOKEN
  from lIter
order by CLE, TOKEN;"	"with  /* CSV LINE PARSER : KEY */
lInputLines as
(select 'A' as CLE, 'AA;BB;CC;1A;56;ERE'     as LINE from dual union all
 select 'B' as CLE, 'B1;B2;B3'               as LINE from dual),
lIter (INDICE, CLE, LINE, TOKEN) as (
select 2                                      as INDICE,
       CLE                                    as CLE,
       LINE                                   as LINE,
       REGEXP_SUBSTR(LINE, '[^;]+', 1, 1)     as TOKEN
 from lInputLines
union all
select b.INDICE + 1                                 as INDICE,
       a.CLE                                        as CLE,
       a.LINE                                       as LINE,
       REGEXP_SUBSTR(b.LINE, '[^;]+', 1, b.INDICE)  as TOKEN
 from lInputLines a
      join lIter b
        on b.CLE = a.CLE
       and REGEXP_SUBSTR(b.LINE, '[^;]+', 1, b.INDICE) is not null)
--CYCLE CLE SET IS_CYCLE TO 1 DEFAULT 0
select INDICE, CLE, /*LINE,*/ TOKEN
  from lIter
order by CLE, TOKEN;"	"with       /* CSV LINE PARSER */
lInputLines as
(select 'AA;BB;CC;1A;56;ERE'     as LINE from dual),
lIter (INDICE, LINE, TOKEN) as (
select 2                                      as INDICE,
       LINE                                   as LINE,
       REGEXP_SUBSTR(LINE, '[^;]+', 1, 1)     as TOKEN
 from lInputLines
union all
select b.INDICE + 1                                 as INDICE,
       a.LINE                                       as LINE,
       REGEXP_SUBSTR(b.LINE, '[^;]+', 1, b.INDICE)  as TOKEN
 from lInputLines a
      join lIter b
        on REGEXP_SUBSTR(b.LINE, '[^;]+', 1, b.INDICE) is not null)
--CYCLE CLE SET IS_CYCLE TO 1 DEFAULT 0
select INDICE,/*LINE,*/ TOKEN
  from lIter
order by TOKEN;
"	"with       /* CSV LINE PARSER */
lInputLines as
(select 1 as id, 'C:\Users\Connor\Presentations\2002\254old.ppt' || '\'     as LINE from dual
 union all
 select 2 as id, 'C:\Users\Connor\Presentations\2002\scene_200205a.pdf' || '\'     as LINE from dual
),
lIter (ID, INDICE, LINE, TOKEN) as (
select ID,
       2                                        as INDICE,
       LINE                                     as LINE,
       rtrim(REGEXP_SUBSTR(LINE, '(.*?)\\', 1, 1), '\')     as TOKEN
 from lInputLines
union all
select a.ID,
       b.INDICE + 1                                   as INDICE,
       a.LINE                                         as LINE,
       rtrim(REGEXP_SUBSTR(b.LINE, '(.*?)\\', 1, b.INDICE), '\')  as TOKEN
 from lInputLines a
      join lIter b
        on b.ID = a.ID
       and REGEXP_SUBSTR(b.LINE, '(.*?)\\', 1, b.INDICE ) is not null)
--CYCLE CLE SET IS_CYCLE TO 1 DEFAULT 0
select ID, INDICE,/*LINE,*/ TOKEN
  from lIter
order by ID, indice;

 -- Reverse
with
lPhrase as (
select 'abc' as phrase
  from dual),
lIter (pInd, pPhrase, pLetter) as (
select length(phrase) - 1 as ind, phrase, substr(phrase, length(phrase), 1)
  from lPhrase
union all
  select pInd - 1, pPhrase, substr(pPhrase, pInd, 1)
    from lIter
  where pInd > 0)
select *
  from lIter;"	"-- Reverse : Sole medere pede ede perede melos
with
lPhrase as (
select 'sole medere pede ede perede melos' as phrase
  from dual),
lPhrase2 as (
select phrase, length(phrase) - 1 as length_phrase, substr(phrase, length(phrase), 1) as first_letter
  from lPhrase),
lIter (pInd, pPhrase, pLetter, pCumul) as (
select length_phrase as ind, phrase, first_letter, first_letter
  from lPhrase2
union all
  select pInd - 1, pPhrase, substr(pPhrase, pInd, 1), pCumul || substr(pPhrase, pInd, 1)
    from lIter
  where pInd > 0)
select *
  from lIter
order by pInd asc
fetch first row only;"	"-- Reverse : Sole medere pede ede perede melos
with
lPhrase as (
select 'sole medere pede ede perede melos' as phrase
  from dual),
lPhrase2 as (
select phrase, length(phrase) - 1 as length_phrase, substr(phrase, length(phrase), 1) as first_letter
  from lPhrase),
lIter (pInd, pPhrase, pCumul) as (
select length_phrase as ind, phrase, first_letter
  from lPhrase2
union all
  select pInd - 1, pPhrase, pCumul || substr(pPhrase, pInd, 1)
    from lIter
  where pInd > 0),
lResult1 as (
select pInd, pPhrase, pCumul
  from lIter
order by pInd asc
fetch first row only),
lResult2 as (
select pInd, pPhrase, pCumul, replace(pPhrase, ' ', '') as p1, replace(pCumul, ' ', '') as p2
  from lResult1)
select pInd, pPhrase, pCumul, p1, p2, case when p1 = p2 then 'OK' else 'WRONG' end as palindrome
  from lResult2;

-- ================================================================================================================================
-- POC_TEMPORAL_DATA_COLLAPSE_PACK_EXPAND_UNPACK_OPERATOR.sql
