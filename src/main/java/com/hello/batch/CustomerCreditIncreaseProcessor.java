/*
 * Copyright 2006-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hello.batch;

import com.hello.batch.entity.CustomerCredit;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.lang.Nullable;

import java.math.BigDecimal;

/**
 * Increases customer's credit by a fixed amount.
 *
 * @author Robert Kasanicky
 */
@Slf4j
public class CustomerCreditIncreaseProcessor implements ItemProcessor<CustomerCredit, CustomerCredit> {

	public static final BigDecimal FIXED_AMOUNT = new BigDecimal("5");

	@Nullable
	@Override
	public CustomerCredit process(CustomerCredit item) throws Exception {
		log.info("Processing item {}", item);
		return item.increaseCreditBy(FIXED_AMOUNT);
	}

}
