USE WAREHOUSE DEFAULT_WH;

drop table PUBLIC.ps_map_traffic_group;
create table PUBLIC.ps_map_traffic_group (
traffic_group varchar
, channel_group varchar
, channel varchar
, subchannel varchar
, target_device varchar
, source_medium varchar
, traffic_type varchar
);

INSERT INTO PUBLIC.ps_map_traffic_group (
    traffic_group, 
    channel_group, 
    channel, 
    subchannel, 
    target_device, 
    source_medium, 
    traffic_type
) 
VALUES 
('PAID', 'SEM Brand', 'SEM', 'Brand', NULL, NULL, NULL),
('PAID', 'SEM NonBrand', 'SEM', 'Action', NULL, NULL, NULL),
('PAID', 'SEM NonBrand', 'SEM', 'Method', NULL, NULL, NULL),
('PAID', 'SEM NonBrand', 'SEM', 'Generic', NULL, NULL, NULL),
('PAID', 'SEM NonBrand', 'SEM', 'Competitor', NULL, NULL, NULL),
('PAID', 'SEM NonBrand', 'SEM', NULL, NULL, NULL, NULL),
('PAID', 'SEM NonBrand', 'SEM', 'SEM', NULL, NULL, NULL),
('PAID', 'SEM NonBrand', 'SEM', 'Reach/Prospecting', NULL, NULL, NULL),
('PAID', 'SEM NonBrand', 'SEM', 'Person/Tutor', NULL, NULL, NULL),
('PAID', 'App Store Marketing Brand', 'APP MARKETING', 'Brand', NULL, 'asa / cpc', NULL),
('PAID', 'App Store Marketing NonBrand', 'APP MARKETING', NULL, NULL, 'asa / cpc', NULL),
('PAID', 'App Store Marketing NonBrand', 'APP MARKETING', 'Reach/Prospecting', NULL, 'asa / cpc', NULL),
('PAID', 'App Store Marketing NonBrand', 'APP MARKETING', 'APP', NULL, 'asa / cpc', NULL),
('PAID', 'App Store Marketing NonBrand', 'APP MARKETING', 'RM for NPC', NULL, 'asa / cpc', NULL),
('PAID', 'App Store Marketing NonBrand', 'APP MARKETING', 'Generic', NULL, 'asa / cpc', NULL),
('PAID', 'Paid Social & Display Prospecting', 'PAID SOCIAL', 'Reach/Prospecting', NULL, NULL, NULL),
('PAID', 'Paid Social & Display Prospecting', 'DISPLAY', 'Reach/Prospecting', NULL, NULL, NULL),
('PAID', 'Paid Social & Display Prospecting', 'APP MARKETING', 'Reach/Prospecting', NULL, NULL, NULL),
('PAID', 'Paid Social & Display Remarketing', 'PAID SOCIAL', 'RM for NPC', NULL, NULL, NULL),
('PAID', 'Paid Social & Display Remarketing', 'DISPLAY', 'RM for NPC', NULL, NULL, NULL),
('PAID', 'Paid Social & Display Remarketing', 'APP MARKETING', 'RM for NPC', NULL, NULL, NULL),
('PAID', 'Paid Social & Display Remarketing', 'PAID SOCIAL', 'RM for repeat', NULL, NULL, NULL),
('PAID', 'Paid Social & Display Remarketing', 'DISPLAY', 'RM for repeat', NULL, NULL, NULL),
('PAID', 'Paid Social & Display Remarketing', 'APP MARKETING', 'RM for repeat', NULL, NULL, NULL),
('PAID', 'Paid Social & Display Remarketing', 'PAID SOCIAL', 'RM for NPC', NULL, NULL, NULL),
('PAID', 'Paid Social & Display Remarketing', 'DISPLAY', 'RM for NPC', NULL, NULL, NULL),
('PAID', 'Paid Social & Display Remarketing', 'APP MARKETING', 'RM for NPC', NULL, NULL, NULL);

INSERT INTO PUBLIC.ps_map_traffic_group VALUES ('PAID', 'Affiliates & Partnerships', 'AFFILIATE', NULL, NULL, NULL, NULL);
INSERT INTO PUBLIC.ps_map_traffic_group VALUES ('PAID', 'Affiliates & Partnerships', 'CO-OP/PARTNERSHIP', NULL, NULL, NULL, NULL);
INSERT INTO PUBLIC.ps_map_traffic_group VALUES ('INBOUND', 'Organic', 'ORGANIC', NULL, NULL, NULL, 'Local');
INSERT INTO PUBLIC.ps_map_traffic_group VALUES ('INBOUND', 'Organic', 'ORGANIC', NULL, NULL, NULL, 'Signup');
INSERT INTO PUBLIC.ps_map_traffic_group VALUES ('INBOUND', 'Organic', 'ORGANIC', NULL, NULL, NULL, 'Landings');
INSERT INTO PUBLIC.ps_map_traffic_group VALUES ('INBOUND', 'Organic', 'ORGANIC', NULL, NULL, NULL, 'Help');
INSERT INTO PUBLIC.ps_map_traffic_group VALUES ('INBOUND', 'Organic', 'ORGANIC', NULL, NULL, NULL, 'Placement test');
INSERT INTO PUBLIC.ps_map_traffic_group VALUES ('INBOUND', 'Organic', 'ORGANIC', NULL, NULL, NULL, 'Scholarship');
INSERT INTO PUBLIC.ps_map_traffic_group VALUES ('INBOUND', 'Organic', 'ORGANIC', NULL, NULL, NULL, 'Online Jobs');
INSERT INTO PUBLIC.ps_map_traffic_group VALUES ('INBOUND', 'Organic', 'ORGANIC', NULL, NULL, NULL, 'Learn');
INSERT INTO PUBLIC.ps_map_traffic_group VALUES ('INBOUND', 'Organic', 'ORGANIC', NULL, NULL, NULL, 'Homepage');
INSERT INTO PUBLIC.ps_map_traffic_group VALUES ('INBOUND', 'Organic', 'ORGANIC', NULL, NULL, NULL, 'Messages');
INSERT INTO PUBLIC.ps_map_traffic_group VALUES ('INBOUND', 'Organic', 'ORGANIC', NULL, NULL, NULL, 'Tutors');
INSERT INTO PUBLIC.ps_map_traffic_group VALUES ('INBOUND', 'Organic', 'ORGANIC', NULL, NULL, NULL, 'Course detail pages');
INSERT INTO PUBLIC.ps_map_traffic_group VALUES ('INBOUND', 'Organic', 'ORGANIC', NULL, NULL, NULL, 'B2B hub');
INSERT INTO PUBLIC.ps_map_traffic_group VALUES ('INBOUND', 'Organic', 'ORGANIC', NULL, NULL, NULL, 'B2B subpages');
INSERT INTO PUBLIC.ps_map_traffic_group VALUES ('INBOUND', 'Organic', 'ORGANIC', NULL, NULL, NULL, 'Learning exercises');
INSERT INTO PUBLIC.ps_map_traffic_group VALUES ('INBOUND', 'Organic', 'ORGANIC', NULL, NULL, NULL, 'Local Jobs');
INSERT INTO PUBLIC.ps_map_traffic_group VALUES ('INBOUND', 'Organic', 'ORGANIC', NULL, NULL, NULL, 'Generic Jobs');
INSERT INTO PUBLIC.ps_map_traffic_group VALUES ('INBOUND', 'Organic', 'ORGANIC', NULL, NULL, NULL, 'Signup Jobs');
INSERT INTO PUBLIC.ps_map_traffic_group VALUES ('INBOUND', 'Organic', 'ORGANIC', NULL, NULL, NULL, 'Learn Generic');
INSERT INTO PUBLIC.ps_map_traffic_group VALUES ('INBOUND', 'Organic', 'ORGANIC', NULL, NULL, NULL, 'Other');
INSERT INTO PUBLIC.ps_map_traffic_group VALUES ('INBOUND', 'Organic', 'ORGANIC', NULL, NULL, NULL, 'SEO Campaigns');
INSERT INTO PUBLIC.ps_map_traffic_group VALUES ('INBOUND', 'Organic', 'ORGANIC', NULL, NULL, NULL, 'Learning categories');
INSERT INTO PUBLIC.ps_map_traffic_group VALUES ('INBOUND', 'Organic', 'ORGANIC', NULL, NULL, NULL, 'App');
INSERT INTO PUBLIC.ps_map_traffic_group VALUES ('INBOUND', 'Organic', 'ORGANIC', NULL, NULL, NULL, 'Classes');
INSERT INTO PUBLIC.ps_map_traffic_group VALUES ('INBOUND', 'Organic', 'ORGANIC', NULL, NULL, NULL, 'Online');
INSERT INTO PUBLIC.ps_map_traffic_group VALUES ('INBOUND', 'Organic', 'ORGANIC', NULL, NULL, NULL, 'Calendar');
INSERT INTO PUBLIC.ps_map_traffic_group VALUES ('INBOUND', 'Organic', 'ORGANIC', NULL, NULL, NULL, 'Content Hub');
INSERT INTO PUBLIC.ps_map_traffic_group VALUES ('INBOUND', 'Organic', 'ORGANIC', NULL, NULL, NULL, 'Brand Other');
INSERT INTO PUBLIC.ps_map_traffic_group VALUES ('INBOUND', 'Organic', 'ORGANIC', NULL, NULL, NULL, 'Test Your Vocab');
INSERT INTO PUBLIC.ps_map_traffic_group VALUES ('INBOUND', 'Organic', 'ORGANIC', NULL, NULL, NULL, 'Form');
INSERT INTO PUBLIC.ps_map_traffic_group VALUES ('INBOUND', 'Organic', 'ORGANIC', NULL, NULL, NULL, 'Lessons');
INSERT INTO PUBLIC.ps_map_traffic_group VALUES ('INBOUND', 'Organic', 'ORGANIC', NULL, NULL, NULL, 'Detailed Jobs');
INSERT INTO PUBLIC.ps_map_traffic_group VALUES ('INBOUND', 'Organic', 'ORGANIC', NULL, NULL, NULL, NULL);
INSERT INTO PUBLIC.ps_map_traffic_group VALUES ('INBOUND', 'Content', 'ORGANIC', NULL, NULL, NULL, 'Tutor Blog');
INSERT INTO PUBLIC.ps_map_traffic_group VALUES ('INBOUND', 'Content', 'DIRECT', NULL, NULL, NULL, 'Tutor Blog');
INSERT INTO PUBLIC.ps_map_traffic_group VALUES ('INBOUND', 'Content', 'ORGANIC', NULL, NULL, NULL, 'Blog');
INSERT INTO PUBLIC.ps_map_traffic_group VALUES ('INBOUND', 'Content', 'DIRECT', NULL, NULL, NULL, 'Blog');
INSERT INTO PUBLIC.ps_map_traffic_group VALUES ('INBOUND', 'Content', 'ORGANIC', NULL, NULL, NULL, 'B2B Blog');
INSERT INTO PUBLIC.ps_map_traffic_group VALUES ('INBOUND', 'Content', 'DIRECT', NULL, NULL, NULL, 'B2B Blog');
INSERT INTO PUBLIC.ps_map_traffic_group VALUES ('INBOUND', 'Content', 'ORGANIC', NULL, NULL, NULL, 'Q&A');
INSERT INTO PUBLIC.ps_map_traffic_group VALUES ('INBOUND', 'Content', 'DIRECT', NULL, NULL, NULL, 'Q&A');
INSERT INTO PUBLIC.ps_map_traffic_group VALUES ('DIRECT & BRAND', 'Direct', 'DIRECT', NULL, NULL, NULL, 'Local');
INSERT INTO PUBLIC.ps_map_traffic_group VALUES ('DIRECT & BRAND', 'Direct', 'DIRECT', NULL, NULL, NULL, 'Signup');
INSERT INTO PUBLIC.ps_map_traffic_group VALUES ('DIRECT & BRAND', 'Direct', 'DIRECT', NULL, NULL, NULL, 'Landings');
INSERT INTO PUBLIC.ps_map_traffic_group VALUES ('DIRECT & BRAND', 'Direct', 'DIRECT', NULL, NULL, NULL, 'Help');
INSERT INTO PUBLIC.ps_map_traffic_group VALUES ('DIRECT & BRAND', 'Direct', 'DIRECT', NULL, NULL, NULL, 'Placement test');
INSERT INTO PUBLIC.ps_map_traffic_group VALUES ('DIRECT & BRAND', 'Direct', 'DIRECT', NULL, NULL, NULL, 'Scholarship');
INSERT INTO PUBLIC.ps_map_traffic_group VALUES ('DIRECT & BRAND', 'Direct', 'DIRECT', NULL, NULL, NULL, 'Online Jobs');
INSERT INTO PUBLIC.ps_map_traffic_group VALUES ('DIRECT & BRAND', 'Direct', 'DIRECT', NULL, NULL, NULL, 'Learn');
INSERT INTO PUBLIC.ps_map_traffic_group VALUES ('DIRECT & BRAND', 'Direct', 'DIRECT', NULL, NULL, NULL, 'Homepage');
INSERT INTO PUBLIC.ps_map_traffic_group VALUES ('DIRECT & BRAND', 'Direct', 'DIRECT', NULL, NULL, NULL, 'Messages');
INSERT INTO PUBLIC.ps_map_traffic_group VALUES ('DIRECT & BRAND', 'Direct', 'DIRECT', NULL, NULL, NULL, 'Tutors');
INSERT INTO PUBLIC.ps_map_traffic_group VALUES ('DIRECT & BRAND', 'Direct', 'DIRECT', NULL, NULL, NULL, 'Course detail pages');
INSERT INTO PUBLIC.ps_map_traffic_group VALUES ('DIRECT & BRAND', 'Direct', 'DIRECT', NULL, NULL, NULL, 'B2B hub');
INSERT INTO PUBLIC.ps_map_traffic_group VALUES ('DIRECT & BRAND', 'Direct', 'DIRECT', NULL, NULL, NULL, 'B2B subpages');
INSERT INTO PUBLIC.ps_map_traffic_group VALUES ('DIRECT & BRAND', 'Direct', 'DIRECT', NULL, NULL, NULL, 'Learning exercises');
INSERT INTO PUBLIC.ps_map_traffic_group VALUES ('DIRECT & BRAND', 'Direct', 'DIRECT', NULL, NULL, NULL, 'Local Jobs');
INSERT INTO PUBLIC.ps_map_traffic_group VALUES ('DIRECT & BRAND', 'Direct', 'DIRECT', NULL, NULL, NULL, 'Generic Jobs');
INSERT INTO PUBLIC.ps_map_traffic_group VALUES ('DIRECT & BRAND', 'Direct', 'DIRECT', NULL, NULL, NULL, 'Signup Jobs');
INSERT INTO PUBLIC.ps_map_traffic_group VALUES ('DIRECT & BRAND', 'Direct', 'DIRECT', NULL, NULL, NULL, 'Learn Generic');
INSERT INTO PUBLIC.ps_map_traffic_group VALUES ('DIRECT & BRAND', 'Direct', 'DIRECT', NULL, NULL, NULL, 'Other');
INSERT INTO PUBLIC.ps_map_traffic_group VALUES ('DIRECT & BRAND', 'Direct', 'DIRECT', NULL, NULL, NULL, 'SEO Campaigns');
INSERT INTO PUBLIC.ps_map_traffic_group VALUES ('DIRECT & BRAND', 'Direct', 'DIRECT', NULL, NULL, NULL, 'Learning categories');
INSERT INTO PUBLIC.ps_map_traffic_group VALUES ('DIRECT & BRAND', 'Direct', 'DIRECT', NULL, NULL, NULL, 'App');
INSERT INTO PUBLIC.ps_map_traffic_group VALUES ('DIRECT & BRAND', 'Direct', 'DIRECT', NULL, NULL, NULL, 'Classes');
INSERT INTO PUBLIC.ps_map_traffic_group VALUES ('DIRECT & BRAND', 'Direct', 'DIRECT', NULL, NULL, NULL, 'Online');
INSERT INTO PUBLIC.ps_map_traffic_group VALUES ('DIRECT & BRAND', 'Direct', 'DIRECT', NULL, NULL, NULL, 'Calendar');
INSERT INTO PUBLIC.ps_map_traffic_group VALUES ('DIRECT & BRAND', 'Direct', 'DIRECT', NULL, NULL, NULL, 'Content Hub');
INSERT INTO PUBLIC.ps_map_traffic_group VALUES ('DIRECT & BRAND', 'Direct', 'DIRECT', NULL, NULL, NULL, 'Brand Other');
INSERT INTO PUBLIC.ps_map_traffic_group VALUES ('DIRECT & BRAND', 'Direct', 'DIRECT', NULL, NULL, NULL, 'Test Your Vocab');
INSERT INTO PUBLIC.ps_map_traffic_group VALUES ('DIRECT & BRAND', 'Direct', 'DIRECT', NULL, NULL, NULL, 'Form');
INSERT INTO PUBLIC.ps_map_traffic_group VALUES ('DIRECT & BRAND', 'Direct', 'DIRECT', NULL, NULL, NULL, 'Lessons');
INSERT INTO PUBLIC.ps_map_traffic_group VALUES ('DIRECT & BRAND', 'Direct', 'DIRECT', NULL, NULL, NULL, 'Detailed Jobs');
INSERT INTO PUBLIC.ps_map_traffic_group VALUES ('DIRECT & BRAND', 'Direct', 'DIRECT', NULL, NULL, NULL, NULL);
INSERT INTO PUBLIC.ps_map_traffic_group VALUES ('DIRECT & BRAND', 'Brand', 'BRAND', NULL, NULL, NULL, NULL);
INSERT INTO PUBLIC.ps_map_traffic_group VALUES ('CRM & RAF', 'CRM', 'CRM', NULL, NULL, NULL, NULL);
INSERT INTO PUBLIC.ps_map_traffic_group VALUES ('CRM & RAF', 'RAF', 'WoM/RAF', NULL, NULL, NULL, NULL);
INSERT INTO PUBLIC.ps_map_traffic_group VALUES ('OTHER', 'Other', 'OTHER', NULL, NULL, NULL, NULL);
INSERT INTO PUBLIC.ps_map_traffic_group VALUES ('OTHER', 'Other', 'JOB BOARDS', NULL, NULL, NULL, NULL);
INSERT INTO PUBLIC.ps_map_traffic_group VALUES ('OTHER', 'Other', 'CONTENT MARKETING', NULL, NULL, NULL, NULL);
INSERT INTO PUBLIC.ps_map_traffic_group VALUES ('OTHER', 'Other', 'DISCOVERY', NULL, NULL, NULL, NULL);
INSERT INTO PUBLIC.ps_map_traffic_group VALUES ('OTHER', 'Other', 'REFERRAL', NULL, NULL, NULL, NULL);


select * from PUBLIC.ps_map_traffic_group t1
