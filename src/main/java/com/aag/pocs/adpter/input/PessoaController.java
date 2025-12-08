package com.aag.pocs.adpter.input;

import com.aag.pocs.model.Pessoa;
import com.aag.pocs.repository.PessoaRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class PessoaController {

    @Autowired
    private PessoaRepository pessoaRepository;

    @GetMapping(value = "/pessoas")
    public Pessoa save() {
        Pessoa pessoa = new Pessoa();
        pessoa.setNome("Afranio");
        pessoa.setIdade(35);
        pessoa.setCidade("Parelhas RN");
        return pessoaRepository.save(pessoa);
    }
}
