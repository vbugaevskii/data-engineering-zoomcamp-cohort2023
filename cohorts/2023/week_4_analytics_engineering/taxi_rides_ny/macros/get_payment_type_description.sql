 {#
    This macro returns the description of the payment_type 
#}

{% macro get_payment_type_description(payment_type) -%}

    multiIf(
        {{ payment_type }} = 1, 'Credit card',
        {{ payment_type }} = 2, 'Cash',
        {{ payment_type }} = 3, 'No charge',
        {{ payment_type }} = 4, 'Dispute',
        {{ payment_type }} = 5, 'Unknown',
        {{ payment_type }} = 6, 'Voided trip',
        'Unknown'
    )

{%- endmacro %}

              