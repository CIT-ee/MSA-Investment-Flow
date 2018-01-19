fields = {
    'funding_rounds':{
        'year_range': [ 2010, 2018 ],
        'relationships': {
            'relationships:investors:properties:permalink': 'investor_permalink',
            'relationships:investors:properties:founded_on': 'investor_founded_on',
            'relationships:investors:properties:short_description': 'investor_short_description',
            'relationships:invested_in:properties:permalink': 'investee_permalink',
            'relationships:invested_in:properties:short_description': 'investee_short_description',
            'relationships:invested_in:properties:founded_on': 'investee_founded_on',
            'properties:is_lead_investor': 'is_lead_investor',
            'properties:money_invested_usd': 'money_invested_usd',
            'uuid': 'investment_uuid'
        },
        'properties': [ 'funding_round_uuid', 'investment_type'  ]
    },
}
