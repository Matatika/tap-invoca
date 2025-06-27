"""Stream type classes for tap-invoca."""

from __future__ import annotations

from functools import cached_property

from singer_sdk import typing as th  # JSON Schema typing helpers
from typing_extensions import override

from tap_invoca.client import InvocaStream


class NetworkTransactionStream(InvocaStream):
    """Define network transaction stream."""

    name = "network_transactions"
    primary_keys = ("transaction_id",)
    replication_key = "transaction_id"
    schema = th.PropertiesList(
        th.Property("transaction_id", th.StringType),
        th.Property("corrects_transaction_id", th.StringType),
        th.Property("transaction_type", th.StringType),  # Call, Post Call Event
        th.Property("original_order_id", th.StringType),
        th.Property("advertiser_id", th.StringType),
        th.Property("advertiser_id_from_network", th.StringType),
        th.Property("advertiser_name", th.StringType),
        th.Property("advertiser_campaign_id", th.StringType),
        th.Property("advertiser_campaign_id_from_network", th.StringType),
        th.Property("advertiser_campaign_name", th.StringType),
        th.Property("affiliate_id", th.StringType),
        th.Property("affiliate_id_from_network", th.StringType),
        th.Property("affiliate_name", th.StringType),
        th.Property("affiliate_commissions_ranking", th.StringType),
        th.Property("affiliate_call_volume_ranking", th.StringType),
        th.Property("affiliate_conversion_rate_ranking", th.StringType),
        th.Property(
            "media_type",
            th.StringType,
        ),  # Offline: Business Publication, Offline: Directory, Offline: Free Standing Insert (FSI), Offline: Outdoor, Offline: Radio, Offline: TV, Online: Content / Review Site, Online: Discount / Coupon Site, Online: Lead Form / Co Reg, Online: Other, Online: Search, Pooling  # noqa: E501
        th.Property("call_source_description", th.StringType),
        th.Property("virtual_line_id", th.StringType),
        th.Property("call_result_description_detail", th.StringType),
        th.Property("advertiser_payin_localized", th.StringType),
        th.Property("affiliate_payout_localized", th.StringType),
        th.Property("margin_localized", th.StringType),
        th.Property("call_fee_localized", th.StringType),
        th.Property("advertiser_call_fee_localized", th.StringType),
        th.Property("matching_advertiser_payin_policies", th.StringType),
        th.Property("matching_affiliate_payout_policies", th.StringType),
        th.Property("payout_conditions", th.StringType),
        th.Property("payin_conditions", th.StringType),
        th.Property("city", th.StringType),
        th.Property("region", th.StringType),
        th.Property("qualified_regions", th.StringType),
        th.Property("repeat_calling_phone_number", th.StringType),
        th.Property("calling_phone_number", th.StringType),
        th.Property("mobile", th.StringType),  # Landline, Mobile
        th.Property("duration", th.IntegerType),
        th.Property("connect_duration", th.IntegerType),
        th.Property("ivr_duration", th.IntegerType),
        th.Property("keypresses", th.StringType),
        th.Property("keypress_1", th.StringType),
        th.Property("keypress_2", th.StringType),
        th.Property("keypress_3", th.StringType),
        th.Property("keypress_4", th.StringType),
        th.Property("dynamic_number_pool_referrer_search_engine", th.StringType),
        th.Property("dynamic_number_pool_referrer_search_keywords_id", th.StringType),
        th.Property("dynamic_number_pool_referrer_search_keywords", th.StringType),
        th.Property("dynamic_number_pool_referrer_ad", th.StringType),
        th.Property("dynamic_number_pool_referrer_ad_id", th.StringType),
        th.Property("dynamic_number_pool_referrer_ad_group", th.StringType),
        th.Property("dynamic_number_pool_referrer_ad_group_id", th.StringType),
        th.Property("dynamic_number_pool_referrer_referrer_campaign", th.StringType),
        th.Property("dynamic_number_pool_referrer_referrer_campaign_id", th.StringType),
        th.Property("dynamic_number_pool_referrer_keyword_match_type", th.StringType),
        th.Property("dynamic_number_pool_referrer_param1_name", th.StringType),
        th.Property("dynamic_number_pool_referrer_param1_value", th.StringType),
        th.Property("dynamic_number_pool_referrer_param2_name", th.StringType),
        th.Property("dynamic_number_pool_referrer_param2_value", th.StringType),
        th.Property("dynamic_number_pool_referrer_param3_name", th.StringType),
        th.Property("dynamic_number_pool_referrer_param3_value", th.StringType),
        th.Property("dynamic_number_pool_referrer_param4_name", th.StringType),
        th.Property("dynamic_number_pool_referrer_param4_value", th.StringType),
        th.Property("dynamic_number_pool_referrer_param5_name", th.StringType),
        th.Property("dynamic_number_pool_referrer_param5_value", th.StringType),
        th.Property("dynamic_number_pool_referrer_param6_name", th.StringType),
        th.Property("dynamic_number_pool_referrer_param6_value", th.StringType),
        th.Property("dynamic_number_pool_referrer_param7_name", th.StringType),
        th.Property("dynamic_number_pool_referrer_param7_value", th.StringType),
        th.Property("dynamic_number_pool_referrer_param8_name", th.StringType),
        th.Property("dynamic_number_pool_referrer_param8_value", th.StringType),
        th.Property("dynamic_number_pool_referrer_param9_name", th.StringType),
        th.Property("dynamic_number_pool_referrer_param9_value", th.StringType),
        th.Property("dynamic_number_pool_referrer_param10_name", th.StringType),
        th.Property("dynamic_number_pool_referrer_param10_value", th.StringType),
        th.Property("dynamic_number_pool_referrer_param11_name", th.StringType),
        th.Property("dynamic_number_pool_referrer_param11_value", th.StringType),
        th.Property("dynamic_number_pool_referrer_param12_name", th.StringType),
        th.Property("dynamic_number_pool_referrer_param12_value", th.StringType),
        th.Property("dynamic_number_pool_referrer_param13_name", th.StringType),
        th.Property("dynamic_number_pool_referrer_param13_value", th.StringType),
        th.Property("dynamic_number_pool_referrer_param14_name", th.StringType),
        th.Property("dynamic_number_pool_referrer_param14_value", th.StringType),
        th.Property("dynamic_number_pool_referrer_param15_name", th.StringType),
        th.Property("dynamic_number_pool_referrer_param15_value", th.StringType),
        th.Property("dynamic_number_pool_referrer_param16_name", th.StringType),
        th.Property("dynamic_number_pool_referrer_param16_value", th.StringType),
        th.Property("dynamic_number_pool_referrer_param17_name", th.StringType),
        th.Property("dynamic_number_pool_referrer_param17_value", th.StringType),
        th.Property("dynamic_number_pool_referrer_param18_name", th.StringType),
        th.Property("dynamic_number_pool_referrer_param18_value", th.StringType),
        th.Property("dynamic_number_pool_referrer_param19_name", th.StringType),
        th.Property("dynamic_number_pool_referrer_param19_value", th.StringType),
        th.Property("dynamic_number_pool_referrer_param20_name", th.StringType),
        th.Property("dynamic_number_pool_referrer_param20_value", th.StringType),
        th.Property("dynamic_number_pool_referrer_param21_name", th.StringType),
        th.Property("dynamic_number_pool_referrer_param21_value", th.StringType),
        th.Property("dynamic_number_pool_referrer_param22_name", th.StringType),
        th.Property("dynamic_number_pool_referrer_param22_value", th.StringType),
        th.Property("dynamic_number_pool_referrer_param23_name", th.StringType),
        th.Property("dynamic_number_pool_referrer_param23_value", th.StringType),
        th.Property("dynamic_number_pool_referrer_param24_name", th.StringType),
        th.Property("dynamic_number_pool_referrer_param24_value", th.StringType),
        th.Property("dynamic_number_pool_referrer_param25_name", th.StringType),
        th.Property("dynamic_number_pool_referrer_param25_value", th.StringType),
        th.Property("dynamic_number_pool_referrer_search_type", th.StringType),
        th.Property("dynamic_number_pool_pool_type", th.StringType),  # Custom
        th.Property("dynamic_number_pool_id", th.IntegerType),
        th.Property("start_time_local", th.StringType),
        th.Property("start_time_xml", th.DateTimeType),
        th.Property("start_time_utc", th.IntegerType),
        th.Property("start_time_network_timezone", th.StringType),
        th.Property("start_time_network_timezone_xml", th.DateTimeType),
        th.Property("recording", th.URIType),
        th.Property("corrected_at", th.StringType),
        th.Property("opt_in_SMS", th.IntegerType),
        th.Property("complete_call_id", th.StringType),
        th.Property(
            "transfer_from_type",
            th.StringType,
        ),  # Advertiser Direct, Publisher Direct, N/A
        th.Property("notes", th.StringType),
        th.Property("verified_zip", th.StringType),
        th.Property(
            "hangup_cause",
            th.StringType,
        ),  # Caller: Hang-up, Destination: Hang-up, Destination: No Answer, Destination: Number Not In Service, IVF: Hang-up  # noqa: E501
        th.Property("signal_name", th.StringType),
        th.Property("signal_partner_unique_id", th.StringType),
        th.Property("signal_occurred_at", th.StringType),
        th.Property("signal_source", th.StringType),
        th.Property("revenue", th.StringType),
        th.Property("sale_amount", th.StringType),
        th.Property("destination_phone_number", th.StringType),
        # https://developers.invoca.net/en/latest/api_documentation/transactions_api/network_user.html#marketing-data-signal-parameters
        th.Property(
            "custom_data",
            th.ArrayType(
                th.ObjectType(
                    th.Property("name", th.StringType),
                    th.Property(
                        "data_type",
                        th.StringType,
                    ),  # Category, Long Text, Short Text, Signal
                    th.Property(
                        "source",
                        th.StringType,
                    ),  # Advertiser, AdvertiserCampaign, AffiliateNetwork, AffiliateCampaign, AI, Api, Default, DynamicAttribution, Expression, Import, Ivr, LookupTable, Syndicated, UserOverride, VirtualLine  # noqa: E501
                    th.Property(
                        "value",
                        th.OneOf(th.StringType, th.BooleanType),
                    ),
                ),
            ),
        ),
    ).to_dict()

    # transactions are ordered i.e. latest transaction is the most recent
    is_sorted = True
    check_sorted = False

    next_page_token_jsonpath = "$[-1].transaction_id"  # noqa: S105

    @override
    @cached_property
    def path(self):
        return f'/networks/transactions/{self.config["network_id"]}.json'

    @override
    def get_url_params(self, context, next_page_token):
        params = super().get_url_params(context, next_page_token)
        params["limit"] = 4000
        params["include_columns"] = ",".join(self.schema["properties"])

        if transaction_id := (
            next_page_token or self.get_starting_replication_key_value(context)
        ):
            params["start_after_transaction_id"] = transaction_id

        return params
