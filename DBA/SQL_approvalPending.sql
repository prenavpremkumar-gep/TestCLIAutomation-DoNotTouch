--Approval not started Query
IF OBJECT_ID('tempdb..#Approval') IS NOT NULL
    DROP TABLE #Approval

CREATE table #Approval
(
 DocumentCode bigint,
 DocumentNumber varchar(100),
 DocumentTypeDescription varchar(200),
 WFOrderId Bigint,
 RowNum int,
 DocInstanceId Bigint,
 WfCount int,
 datecreated datetime,
 UseCase varchar(200)
)
insert into #Approval
SELECT doc.DocumentCode,doc.DocumentNumber,t.DocumentTypeDescription [DocType], ord.WFOrderId, 
      ROW_NUMBER() over (PARTITION by ins.DocInstanceId order by ord.WorkflowOrder asc),ins.DocInstanceId,0,doc.datecreated,'ApprovalNotStarted'
from dm_documents doc
inner join DM_DocumentType t on t.DocumentTypeId = doc.DocumentTypeCode
inner join WF_DocumentInstance ins on ins.DocumentCode = doc.DocumentCode
inner join WF_DocumentWorkflowOrder ord on ord.DocInstanceId=ins.DocInstanceId
where doc.DocumentStatus=21 
and doc.IsDeleted=0
and ins.IsActive=1
and ord.IsProcessed=0


--Approval Completed but Document in approval pending
IF OBJECT_ID('tempdb..#ApprovalComplete') IS NOT NULL
    DROP TABLE #ApprovalComplete

CREATE table #ApprovalComplete
(
 DocumentCode bigint,
 DocumentNumber varchar(100),
 DocumentTypeDescription varchar(200),
 WFOrderId Bigint,
 RowNum int,
 DocInstanceId Bigint,
 WfCount int,
 datecreated datetime ,
 UseCase varchar(200)
)
insert into #ApprovalComplete
SELECT doc.DocumentCode,doc.DocumentNumber,t.DocumentTypeDescription [DocType],0,0,ins.DocInstanceId, 
      COUNT(ord.WFOrderId) WfCount, doc.DateCreated,'ApprovalCompleted' 
	  from dm_documents doc
inner join DM_DocumentType t on t.DocumentTypeId = doc.DocumentTypeCode
inner join WF_DocumentInstance ins on ins.DocumentCode = doc.DocumentCode
inner join WF_DocumentWorkflowOrder ord on ord.DocInstanceId=ins.DocInstanceId
where doc.DocumentStatus=21 
and doc.IsDeleted =0
and ins.IsActive=1 
GROUP by doc.DocumentCode,doc.DocumentNumber,t.DocumentTypeDescription, ins.DocInstanceId,doc.DateCreated


IF OBJECT_ID('tempdb..#ApprovalIsProcessed') IS NOT NULL
    DROP TABLE #ApprovalIsProcessed

CREATE table #ApprovalIsProcessed
(
 DocumentCode bigint,
 DocumentNumber varchar(100),
 DocumentTypeDescription varchar(max),
 DocInstanceId Bigint,
 WfCount int
)

insert into #ApprovalIsProcessed
SELECT doc.DocumentCode,doc.DocumentNumber,t.DocumentTypeDescription [DocType], ins.DocInstanceId, 
      COUNT(ord.WFOrderId) WfCount
from dm_documents doc
inner join DM_DocumentType t on t.DocumentTypeId = doc.DocumentTypeCode
inner join WF_DocumentInstance ins on ins.DocumentCode = doc.DocumentCode
inner join WF_DocumentWorkflowOrder ord on ord.DocInstanceId=ins.DocInstanceId
where doc.DocumentStatus=21 
and ins.IsActive=1
and ord.IsProcessed=1
and doc.IsDeleted=0
GROUP by doc.DocumentCode,doc.DocumentNumber,t.DocumentTypeDescription, ins.DocInstanceId



select doc.DocumentCode,doc.DocumentNumber,t.DocumentTypeDescription, 0 WfOrderId,0 RowNum,wf.DocInstanceId, 0 WfCount,doc.datecreated,'WorkflowMissing_Couldbe_RequsterDefaultCostcenterInactiveOrRequesterInactive' Usecase from dm_documents doc
inner join DM_DocumentType t on t.DocumentTypeId = doc.DocumentTypeCode
left join WF_DocumentInstance wf on wf.DocumentCode=doc.DocumentCode
where doc.DocumentStatus=21 and doc.IsDeleted=0 and wf.DocInstanceId is NULL
Union all
select app.* from #Approval app
left join WF_ApprovalLog [Log] on log.WFOrderId = app.WFOrderId
where app.RowNum =1
and Log.WorkflowLogId is null
UNION ALL
select Approval.* 
from #ApprovalComplete Approval
inner join #ApprovalIsProcessed ApprovalProcessed on Approval.DocumentCode = ApprovalProcessed.DocumentCode
where Approval.WfCount = ApprovalProcessed.WfCount





