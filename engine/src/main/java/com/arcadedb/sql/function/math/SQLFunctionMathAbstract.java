/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */
package com.arcadedb.sql.function.math;

import com.arcadedb.sql.function.SQLFunctionConfigurableAbstract;

import java.math.BigDecimal;

/**
 * Abstract class for math function.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 *
 */
public abstract class SQLFunctionMathAbstract extends SQLFunctionConfigurableAbstract {

	public SQLFunctionMathAbstract(String iName, int iMinParams, int iMaxParams) {
		super(iName, iMinParams, iMaxParams);
	}

	protected Number getContextValue(Object iContext, final Class<? extends Number> iClass) {
		if (iClass != iContext.getClass()) {
			// CHANGE TYPE
			if (iClass == Long.class)
				iContext = new Long(((Number) iContext).longValue());
			else if (iClass == Short.class)
				iContext = new Short(((Number) iContext).shortValue());
			else if (iClass == Float.class)
				iContext = new Float(((Number) iContext).floatValue());
			else if (iClass == Double.class)
				iContext = new Double(((Number) iContext).doubleValue());
		}

		return (Number) iContext;
	}

	protected Class<? extends Number> getClassWithMorePrecision(final Class<? extends Number> iClass1,
			final Class<? extends Number> iClass2) {
		if (iClass1 == iClass2)
			return iClass1;

		if (iClass1 == Integer.class
				&& (iClass2 == Long.class || iClass2 == Float.class || iClass2 == Double.class || iClass2 == BigDecimal.class))
			return iClass2;
		else if (iClass1 == Long.class && (iClass2 == Float.class || iClass2 == Double.class || iClass2 == BigDecimal.class))
			return iClass2;
		else if (iClass1 == Float.class && (iClass2 == Double.class || iClass2 == BigDecimal.class))
			return iClass2;

		return iClass1;
	}

	@Override
	public boolean aggregateResults() {
		return configuredParameters.length == 1;
	}
}
