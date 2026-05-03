/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package e2e

import (
	"fmt"
	"os/exec"
	"testing"

	. "github.com/onsi/ginkgo/v2" //nolint:revive,staticcheck
	. "github.com/onsi/gomega"    //nolint:revive,staticcheck

	"github.com/osac-project/osac-operator/test/utils"
)

var _ = BeforeSuite(func() {
	By("installing cert-manager")
	Expect(utils.InstallCertManager()).To(Succeed())

	By("installing CRDs")
	cmd := exec.Command("make", "install")
	_, err := utils.Run(cmd)
	Expect(err).NotTo(HaveOccurred())
})

var _ = AfterSuite(func() {
	By("uninstalling cert-manager")
	utils.UninstallCertManager()
})

// Run e2e tests using the Ginkgo runner.
func TestE2E(t *testing.T) {
	RegisterFailHandler(Fail)
	_, _ = fmt.Fprintf(GinkgoWriter, "Starting osac-operator suite\n")
	RunSpecs(t, "e2e suite")
}
