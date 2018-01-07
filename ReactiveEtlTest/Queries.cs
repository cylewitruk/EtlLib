namespace ReactiveEtlTest
{
    public static class Queries
    {
        public static string JournalEntriesQuery = @"select * from (
	                                select
		                                'gl' as source,
		                                gltr.booked_at_utc,
		                                gltr.transaction_record_sequence_number as sequence_number,
		                                gltp.merchant_ext_id as merchant_id,
		                                gltp.creditor_ext_id as creditor_id,
		                                gltr.debit_amount as debit_amount,
		                                gltr.credit_amount as credit_amount,
		                                gla.account_ext_id as account_number,
		                                glj.journal_ext_id as journal_id
	                                from
		                                gl.transaction_record gltr with (nolock)
		                                inner join gl.account gla with (nolock) on gla.account_sk = gltr.account_sk
		                                inner join gl.journal glj with (nolock) on glj.journal_sk = gltr.journal_sk
		                                left join gl.transaction_profile gltp with (nolock) on gltp.transaction_profile_sk = gltr.transaction_profile_sk
	                                union all
	                                select
		                                'pbs' as source,
		                                j.created_timestamp as booked_at_utc,
		                                je.journal_entry_ext_id as sequence_number,
		                                j.paynova_account_id as merchant_id,
		                                md.creditor_id as creditor_id,
		                                999.99,
		                                999.99,
		                                ja.journal_account_number as account_number,
		                                jt.journal_type_ext_id
	                                from
		                                pdw_base.journal j with (nolock)
		                                inner join pdw_base.journal_entry je with (nolock) on j.journal_sk = je.journal_sk
		                                inner join pdw_base.merchant_detail md with (nolock) on md.merchant_sk = j.merchant_sk
		                                inner join pdw_base.journal_account ja with (nolock) on ja.journal_account_sk = je.journal_account_sk
		                                inner join pdw_base.journal_type jt with (nolock) on j.journal_type_sk = jt.journal_type_sk
                                ) as result
                            where 
                                result.booked_at_utc between @FromDate and @ToDate";
    }
}