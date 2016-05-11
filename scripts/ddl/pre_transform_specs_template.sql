DELETE FROM `monasca_transform`.`pre_transform_specs`;

INSERT IGNORE INTO `monasca_transform`.`pre_transform_specs`
(`event_type`,
`pre_transform_spec`)
VALUES
