package de.knibel.outbox.architecture;

import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;
import de.knibel.outbox.jdbc.rowmapper.PayloadMapper;
import de.knibel.outbox.jdbc.selection.SelectionStrategy;
import de.knibel.outbox.repository.AcknowledgementHandler;
import de.knibel.outbox.repository.OutboxDataMapper;
import de.knibel.outbox.transport.MessageTransport;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;
import static com.tngtech.archunit.library.dependencies.SlicesRuleDefinition.slices;

/**
 * Architecture tests that enforce the modular structure of the outbox adapter.
 *
 * <p>Key architectural constraints:
 * <ul>
 *   <li>The {@code repository} package defines a persistence-independent interface.
 *   <li>The {@code transport} package defines a broker-independent interface.
 *   <li>Strategy implementations implement their respective interfaces.
 *   <li>There are no cyclic dependencies between top-level packages.
 * </ul>
 */
@AnalyzeClasses(packages = "de.knibel.outbox", importOptions = ImportOption.DoNotIncludeTests.class)
class ArchitectureTest {

    // ── Repository independence ──────────────────────────────────────────────

    @ArchTest
    static final ArchRule repository_should_not_depend_on_jdbc =
            noClasses().that().resideInAPackage("..repository..")
                    .should().dependOnClassesThat().resideInAPackage("..jdbc..");

    @ArchTest
    static final ArchRule repository_should_not_depend_on_transport =
            noClasses().that().resideInAPackage("..repository..")
                    .should().dependOnClassesThat().resideInAPackage("..transport..");

    // ── Transport independence ───────────────────────────────────────────────

    @ArchTest
    static final ArchRule transport_should_not_depend_on_jdbc =
            noClasses().that().resideInAPackage("..transport..")
                    .should().dependOnClassesThat().resideInAPackage("..jdbc..");

    @ArchTest
    static final ArchRule transport_should_not_depend_on_polling =
            noClasses().that().resideInAPackage("..transport..")
                    .should().dependOnClassesThat().resideInAPackage("..polling..");

    // ── Strategy pattern enforcement ─────────────────────────────────────────

    @ArchTest
    static final ArchRule selection_strategies_implement_interface =
            classes().that().resideInAPackage("..jdbc.selection..")
                    .and().areNotInterfaces()
                    .and().doNotHaveSimpleName("SelectionQuery")
                    .should().implement(SelectionStrategy.class);

    @ArchTest
    static final ArchRule payload_mappers_implement_interface =
            classes().that().resideInAPackage("..jdbc.rowmapper..")
                    .and().areNotInterfaces()
                    .should().implement(PayloadMapper.class)
                    .orShould().implement(OutboxDataMapper.class);

    @ArchTest
    static final ArchRule acknowledgement_handlers_implement_interface =
            classes().that().resideInAPackage("..jdbc.acknowledgement..")
                    .and().areNotInterfaces()
                    .should().implement(AcknowledgementHandler.class);

    @ArchTest
    static final ArchRule kafka_transport_implements_message_transport =
            classes().that().resideInAPackage("..transport.kafka..")
                    .and().areNotInterfaces()
                    .and().areNotAssignableTo(Throwable.class)
                    .should().implement(MessageTransport.class);

    // ── No cyclic dependencies ──────────────────────────────────────────────

    @ArchTest
    static final ArchRule no_package_cycles =
            slices().matching("de.knibel.outbox.(*)..").should().beFreeOfCycles();
}
