/**
 * Copyright 2016-2020 Tenefit. All rights reserved.
 */
package com.tenefit.demo.microservice.temperatureConverter;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class TemperatureUnitTest
{
    @Test
    public void shouldConvertCelsiusToCelsius() throws Exception
    {
        assertEquals(0, TemperatureUnit.C.convertTo(0, TemperatureUnit.C));
    }

    @Test
    public void shouldConvertCelsiusToFahrenheit() throws Exception
    {
        assertEquals(32, TemperatureUnit.C.convertTo(0, TemperatureUnit.F));
    }

    @Test
    public void shouldConvertCelsiusToKelvin() throws Exception
    {
        assertEquals(273, TemperatureUnit.C.convertTo(0, TemperatureUnit.K));
    }

    @Test
    public void shouldConvertFahrenheitToCelsius() throws Exception
    {
        assertEquals(-18, TemperatureUnit.F.convertTo(0, TemperatureUnit.C));
    }

    @Test
    public void shouldConvertFahrenheitToFahrenheit() throws Exception
    {
        assertEquals(0, TemperatureUnit.F.convertTo(0, TemperatureUnit.F));
    }

    @Test
    public void shouldConvertFahrenheitToKelvin() throws Exception
    {
        assertEquals(255, TemperatureUnit.F.convertTo(0, TemperatureUnit.K));
    }

    @Test
    public void shouldConvertKelvinToCelsius() throws Exception
    {
        assertEquals(-273, TemperatureUnit.K.convertTo(0, TemperatureUnit.C));
    }

    @Test
    public void shouldConvertKelvinToFahrenheit() throws Exception
    {
        assertEquals(-460, TemperatureUnit.K.convertTo(0, TemperatureUnit.F));
    }

    @Test
    public void shouldConvertKelvinToKelvin() throws Exception
    {
        assertEquals(0, TemperatureUnit.K.convertTo(0, TemperatureUnit.K));
    }
}
