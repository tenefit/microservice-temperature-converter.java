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
        assertEquals(0, TemperatureUnit.C.canonicalize(0, TemperatureUnit.C));
    }

    @Test
    public void shouldConvertCelsiusToFahrenheit() throws Exception
    {
        assertEquals(32, TemperatureUnit.F.canonicalize(0, TemperatureUnit.C));
    }

    @Test
    public void shouldConvertCelsiusToKelvin() throws Exception
    {
        assertEquals(273, TemperatureUnit.K.canonicalize(0, TemperatureUnit.C));
    }

    @Test
    public void shouldConvertFahrenheitToCelsius() throws Exception
    {
        assertEquals(-18, TemperatureUnit.C.canonicalize(0, TemperatureUnit.F));
    }

    @Test
    public void shouldConvertFahrenheitToFahrenheit() throws Exception
    {
        assertEquals(0, TemperatureUnit.F.canonicalize(0, TemperatureUnit.F));
    }

    @Test
    public void shouldConvertFahrenheitToKelvin() throws Exception
    {
        assertEquals(255, TemperatureUnit.K.canonicalize(0, TemperatureUnit.F));
    }

    @Test
    public void shouldConvertKelvinToCelsius() throws Exception
    {
        assertEquals(-273, TemperatureUnit.C.canonicalize(0, TemperatureUnit.K));
    }

    @Test
    public void shouldConvertKelvinToFahrenheit() throws Exception
    {
        assertEquals(-460, TemperatureUnit.F.canonicalize(0, TemperatureUnit.K));
    }

    @Test
    public void shouldConvertKelvinToKelvin() throws Exception
    {
        assertEquals(0, TemperatureUnit.K.canonicalize(0, TemperatureUnit.K));
    }
}
