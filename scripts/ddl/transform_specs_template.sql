DELETE FROM `monasca_transform`.`transform_specs`;

INSERT IGNORE INTO `monasca_transform`.`transform_specs`
(`metric_id`,
`transform_spec`)
VALUES
