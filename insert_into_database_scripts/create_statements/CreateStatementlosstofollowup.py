def CreateStatementltfu():
    create_script = ''' CREATE TABLE IF NOT EXISTS loss_to_follow_up(
            study_id text PRIMARY KEY,
            site integer,
            Reasons_for_LTFU text,
            other_LTFU text,
            cause_of_death_known text,
            cause_of_death text,
            reasons_for_refusals text,
            other_reason_for_refusal text
                        )
'''

    return create_script