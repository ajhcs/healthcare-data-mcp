from servers.form_990_facts.fact_store import extract_filing


AGGREGATE_ROW_XML = b"""<?xml version="1.0" encoding="UTF-8"?>
<Return xmlns="http://www.irs.gov/efile">
  <ReturnHeader>
    <TaxPeriodEndDt>2024-12-31</TaxPeriodEndDt>
    <ReturnTypeCd>990</ReturnTypeCd>
    <Filer>
      <EIN>741180155</EIN>
      <BusinessName><BusinessNameLine1Txt>THE METHODIST HOSPITAL</BusinessNameLine1Txt></BusinessName>
    </Filer>
  </ReturnHeader>
  <ReturnData>
    <IRS990>
      <CYTotalRevenueAmt>100</CYTotalRevenueAmt>
      <CYTotalExpensesAmt>90</CYTotalExpensesAmt>
      <NetAssetsOrFundBalancesEOYAmt>10</NetAssetsOrFundBalancesEOYAmt>
      <Form990PartVIISectionAGrp>
        <PersonNm>NINE OFFICERDIR-SEE METHODIST</PersonNm>
        <TitleTxt>GROUP RETURN</TitleTxt>
        <KeyEmployeeInd>X</KeyEmployeeInd>
        <ReportableCompFromOrgAmt>9000000</ReportableCompFromOrgAmt>
        <ReportableCompFromRltdOrgAmt>0</ReportableCompFromRltdOrgAmt>
        <OtherCompensationAmt>1000000</OtherCompensationAmt>
      </Form990PartVIISectionAGrp>
      <Form990PartVIISectionAGrp>
        <PersonNm>JANE DOE</PersonNm>
        <TitleTxt>PRESIDENT AND CEO</TitleTxt>
        <OfficerInd>X</OfficerInd>
        <ReportableCompFromOrgAmt>2000000</ReportableCompFromOrgAmt>
        <ReportableCompFromRltdOrgAmt>500000</ReportableCompFromRltdOrgAmt>
        <OtherCompensationAmt>250000</OtherCompensationAmt>
      </Form990PartVIISectionAGrp>
    </IRS990>
  </ReturnData>
</Return>
"""


def test_top_executive_ignores_explicit_aggregate_part_vii_rows() -> None:
    filing = extract_filing(AGGREGATE_ROW_XML)
    executive = next(fact for fact in filing.facts if fact.metric_key == "top_reported_executive")

    assert executive.value == 2_750_000
    assert executive.details is not None
    assert executive.details["name"] == "JANE DOE"
    assert executive.details["title"] == "PRESIDENT AND CEO"
